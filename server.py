"""
FitGirl Download Server - FastAPI
Downloads games from FitGirl and serves files to client
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from hosts import extract_mirror_links, resolve_host, HOSTER_LABELS, PREFERRED_HOSTS

# ===== LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("fitgirl")
console = Console()

# ===== SETTINGS =====
DOWNLOAD_PATH = Path(os.getenv("FITGIRL_DOWNLOAD_PATH", "./downloads"))
STATE_FILE = DOWNLOAD_PATH / "state.json"
MAX_CONCURRENT_DOWNLOADS = 2
CHUNK_SIZE = 1024 * 1024  # 1MB
MAX_RETRIES = 5
RETRY_DELAY = 3

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)


# ===== MODELS =====
class StartDownloadRequest(BaseModel):
    url: str
    mirror: str | None = None


# ===== APP STATE =====
games_state: dict[str, dict] = {}
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)


# ===== LIFESPAN =====
@asynccontextmanager
async def lifespan(app):
    log.info("SERVER STARTING")
    await load_state()
    yield
    log.info("SERVER STOPPING")


app = FastAPI(title="FitGirl Download Server", version="2.0.0", lifespan=lifespan)


# ===== STATE MANAGEMENT =====
async def save_state():
    try:
        async with aiofiles.open(STATE_FILE, 'w') as f:
            await f.write(json.dumps(games_state, indent=2, ensure_ascii=False))
    except Exception as e:
        log.error(f"Failed to save state: {e}")


async def load_state():
    global games_state
    if STATE_FILE.exists():
        try:
            async with aiofiles.open(STATE_FILE, 'r') as f:
                content = await f.read()
                games_state = json.loads(content) if content else {}
            log.info(f"Loaded {len(games_state)} games from state")
        except Exception as e:
            log.error(f"Failed to load state: {e}")
            games_state = {}


def generate_game_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def get_short_name(filename: str) -> str:
    part_match = re.search(r'(part\d+)', filename, re.IGNORECASE)
    return part_match.group(1) if part_match else filename[:20]


# ===== LINK EXTRACTION =====
async def extract_download_links(url: str, mirror: str | None = None) -> tuple[str, list[dict]]:
    log.info(f"Extracting links from: {url}")

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=400, detail=f"Failed: HTTP {resp.status}")
            html = await resp.text()

        soup = BeautifulSoup(html, 'html.parser')
        title_tag = soup.find('h1', class_='entry-title')
        game_name = title_tag.text.strip() if title_tag else "Unknown Game"
        log.info(f"Game: {game_name}")

        # كل الـ hosters اللي في الصفحة بيتكتشفوا ديناميكيًا
        links_by_host = extract_mirror_links(soup)
        log.info(f"Mirrors found: {', '.join(links_by_host) or 'none'}")
        if not links_by_host:
            raise HTTPException(status_code=400, detail="No download mirrors found on the page")

        # ترتيب اختيار الـ mirror: mirror محدد من المستخدم (لو موجود) ثم preferred ثم الباقي
        if mirror and mirror in links_by_host:
            candidates = [mirror]
        else:
            candidates = [h for h in PREFERRED_HOSTS if h in links_by_host]
            candidates += [h for h in links_by_host if h not in PREFERRED_HOSTS]

        for host in candidates:
            links = links_by_host[host]
            label = HOSTER_LABELS.get(host, host)
            log.info(f"Trying mirror [{label}] ({len(links)} files)")
            download_links = await resolve_host(session, links, host, referer=url)
            log.info(f"  -> {len(download_links)}/{len(links)} direct links")
            if download_links:
                return game_name, download_links

        raise HTTPException(status_code=400, detail="No direct links could be resolved from any mirror")


# ===== FILE DOWNLOAD WITH RETRY =====
async def download_file(url: str, filepath: Path, game_id: str, filename: str, progress: Progress, task_id):
    short = get_short_name(filename)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with download_semaphore:
                games_state[game_id]["files"][filename]["status"] = "downloading"

                async with aiohttp.ClientSession(headers=HEADERS) as session:
                    downloaded_size = filepath.stat().st_size if filepath.exists() else 0
                    req_headers = {}
                    if downloaded_size > 0:
                        req_headers['Range'] = f'bytes={downloaded_size}-'

                    async with session.get(
                        url, headers=req_headers,
                        timeout=aiohttp.ClientTimeout(total=None, sock_read=120)
                    ) as resp:

                        if resp.status == 416:
                            total_size = downloaded_size
                            games_state[game_id]["files"][filename]["total_size"] = total_size
                            games_state[game_id]["files"][filename]["downloaded_size"] = total_size
                            progress.update(task_id, total=total_size, completed=total_size)
                            break

                        if resp.status not in [200, 206]:
                            raise Exception(f"HTTP {resp.status}")

                        if 'text/html' in resp.headers.get('Content-Type', ''):
                            raise Exception("Got HTML instead of file")

                        total_size = int(resp.headers.get('content-length', 0))
                        if resp.status == 206:
                            total_size += downloaded_size

                        games_state[game_id]["files"][filename]["total_size"] = total_size
                        progress.update(task_id, total=total_size, completed=downloaded_size,
                                       description=f"[yellow]↓ {short}")

                        mode = 'ab' if downloaded_size > 0 else 'wb'
                        async with aiofiles.open(filepath, mode) as f:
                            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                                await f.write(chunk)
                                downloaded_size += len(chunk)
                                games_state[game_id]["files"][filename]["downloaded_size"] = downloaded_size
                                progress.update(task_id, completed=downloaded_size)

            # Verify download
            if filepath.exists():
                actual = filepath.stat().st_size
                expected = games_state[game_id]["files"][filename]["total_size"]
                if expected > 0 and actual < expected:
                    raise Exception(f"Incomplete: {actual}/{expected}")

            # Mark as ready
            file_size = filepath.stat().st_size
            games_state[game_id]["files"][filename]["status"] = "ready"
            games_state[game_id]["files"][filename]["file_path"] = str(filepath)
            games_state[game_id]["files"][filename]["file_size"] = file_size
            await save_state()

            progress.update(task_id, description=f"[green]✓ {short}")
            await check_game_complete(game_id)
            return

        except Exception as e:
            log.warning(f"  [{short}] Attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                progress.update(task_id, description=f"[yellow]⟳ {short} retry {attempt+1}")
                await asyncio.sleep(RETRY_DELAY * attempt)
            else:
                progress.update(task_id, description=f"[red]✗ {short}")
                games_state[game_id]["files"][filename]["status"] = "error"
                games_state[game_id]["files"][filename]["error"] = str(e)
                await save_state()
                raise


async def check_game_complete(game_id: str):
    files = games_state[game_id]["files"]
    all_ready = all(f["status"] == "ready" for f in files.values())
    any_error = any(f["status"] == "error" for f in files.values())

    if any_error:
        games_state[game_id]["status"] = "error"
    elif all_ready:
        games_state[game_id]["status"] = "ready"
    await save_state()


# ===== GAME DOWNLOAD =====
async def start_game_download(game_id: str, url: str, mirror: str | None = None):
    try:
        game_name, download_links = await extract_download_links(url, mirror)

        game_path = DOWNLOAD_PATH / game_id
        game_path.mkdir(parents=True, exist_ok=True)

        games_state[game_id]["name"] = game_name
        games_state[game_id]["status"] = "downloading"

        for file_info in download_links:
            filename = file_info['name']
            games_state[game_id]["files"][filename] = {
                "url": file_info['url'],
                "hoster": file_info.get('hoster'),
                "status": "pending",
                "total_size": 0,
                "downloaded_size": 0,
                "file_path": "",
                "file_size": 0,
                "error": None
            }
        await save_state()

        # Disable logging during progress display
        log.setLevel(logging.WARNING)

        console.print(f"\n[bold green]Game:[/] {game_name}")
        console.print(f"[bold green]Files:[/] {len(download_links)}\n")

        with Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(30),
            "[progress.percentage]{task.percentage:>3.0f}%",
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=4
        ) as progress:
            tasks = []
            for filename in games_state[game_id]["files"]:
                short = get_short_name(filename)
                filepath = game_path / filename
                file_url = games_state[game_id]["files"][filename]["url"]
                task_id = progress.add_task(f"[dim]⋯ {short}", total=0)
                task = asyncio.create_task(
                    download_file(file_url, filepath, game_id, filename, progress, task_id)
                )
                tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=True)

        # Re-enable logging
        log.setLevel(logging.INFO)

        console.print(f"\n[bold green]DOWNLOAD COMPLETE: {game_name}[/]\n")

    except Exception as e:
        log.setLevel(logging.INFO)
        log.error(f"Download failed: {e}")
        games_state[game_id]["status"] = "error"
        games_state[game_id]["error"] = str(e)
        await save_state()


# ===== API ENDPOINTS =====
@app.post("/start")
async def start_download_endpoint(request: StartDownloadRequest, background_tasks: BackgroundTasks):
    log.info(f"POST /start - {request.url}")
    game_id = generate_game_id(request.url)

    if game_id in games_state and games_state[game_id]["status"] not in ["error"]:
        return JSONResponse({
            "game_id": game_id,
            "message": "Game already exists",
            "status": games_state[game_id]["status"]
        })

    games_state[game_id] = {
        "game_id": game_id, "name": "Loading...", "url": request.url,
        "mirror": request.mirror,
        "status": "pending", "created_at": datetime.now().isoformat(),
        "files": {}, "error": None
    }
    await save_state()

    background_tasks.add_task(start_game_download, game_id, request.url, request.mirror)
    return {"game_id": game_id, "message": "Download started", "mirror": request.mirror}


@app.get("/games")
async def list_games():
    return {
        "games": [
            {
                "game_id": g["game_id"], "name": g["name"], "status": g["status"],
                "created_at": g["created_at"], "files_count": len(g["files"]),
                "error": g.get("error")
            }
            for g in games_state.values()
        ]
    }


@app.get("/status/{game_id}")
async def get_status(game_id: str):
    if game_id not in games_state:
        raise HTTPException(status_code=404, detail="Game not found")

    game = games_state[game_id]
    total_size = sum(f["total_size"] for f in game["files"].values())
    downloaded_size = sum(f["downloaded_size"] for f in game["files"].values())

    return {
        "game_id": game["game_id"],
        "name": game["name"],
        "status": game["status"],
        "progress": round((downloaded_size / total_size * 100) if total_size > 0 else 0, 2),
        "total_size": total_size,
        "downloaded_size": downloaded_size,
        "files_status": {
            name: {
                "status": f["status"],
                "total_size": f["total_size"],
                "downloaded_size": f["downloaded_size"],
                "progress": round((f["downloaded_size"] / f["total_size"] * 100) if f["total_size"] > 0 else 0, 2)
            }
            for name, f in game["files"].items()
        },
        "error": game.get("error")
    }


@app.get("/files/{game_id}")
async def list_files(game_id: str):
    if game_id not in games_state:
        raise HTTPException(status_code=404, detail="Game not found")

    game = games_state[game_id]
    return {
        "game_id": game_id,
        "name": game["name"],
        "status": game["status"],
        "files": [
            {
                "filename": name,
                "status": f["status"],
                "file_size": f.get("file_size", f["total_size"]),
                "download_url": f"/download/{game_id}/{name}" if f["status"] == "ready" else None
            }
            for name, f in game["files"].items()
        ]
    }


@app.get("/download/{game_id}/{filename}")
async def download_file_endpoint(game_id: str, filename: str, request: Request):
    try:
        if game_id not in games_state:
            raise HTTPException(status_code=404, detail="Game not found")
        if filename not in games_state[game_id]["files"]:
            raise HTTPException(status_code=404, detail="File not found")

        file_info = games_state[game_id]["files"][filename]
        if file_info["status"] != "ready":
            raise HTTPException(status_code=400, detail=f"Not ready: {file_info['status']}")

        file_path = Path(file_info["file_path"])
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"File missing: {file_path}")

        file_size = file_path.stat().st_size
        safe_filename = quote(filename)
        range_header = request.headers.get("range")

        if range_header:
            range_match = re.match(r'bytes=(\d+)-(\d*)', range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2)) if range_match.group(2) else file_size - 1

                if start >= file_size:
                    raise HTTPException(status_code=416, detail="Range not satisfiable")

                async def stream_range():
                    async with aiofiles.open(file_path, 'rb') as f:
                        await f.seek(start)
                        remaining = end - start + 1
                        while remaining > 0:
                            chunk = await f.read(min(CHUNK_SIZE, remaining))
                            if not chunk:
                                break
                            remaining -= len(chunk)
                            yield chunk

                return StreamingResponse(
                    stream_range(), status_code=206,
                    media_type="application/octet-stream",
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{file_size}",
                        "Content-Length": str(end - start + 1),
                        "Accept-Ranges": "bytes",
                        "Content-Disposition": f"attachment; filename*=UTF-8''{safe_filename}"
                    }
                )

        async def stream_file():
            async with aiofiles.open(file_path, 'rb') as f:
                while chunk := await f.read(CHUNK_SIZE):
                    yield chunk

        return StreamingResponse(
            stream_file(),
            media_type="application/octet-stream",
            headers={
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes",
                "Content-Disposition": f"attachment; filename*=UTF-8''{safe_filename}"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Download error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/games/{game_id}")
async def delete_game(game_id: str):
    if game_id not in games_state:
        raise HTTPException(status_code=404, detail="Game not found")

    game_path = DOWNLOAD_PATH / game_id
    if game_path.exists():
        shutil.rmtree(game_path)

    del games_state[game_id]
    await save_state()
    return {"message": "Deleted"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    log.info("Starting server on port 7284...")
    uvicorn.run(app, host="0.0.0.0", port=7284, access_log=False)
