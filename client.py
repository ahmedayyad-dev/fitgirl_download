"""
FitGirl Download Client
Downloads game files from VPS server with RAR extraction
"""

import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Optional

import aiohttp
from rich.console import Console
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn,
    DownloadColumn, TransferSpeedColumn, TimeRemainingColumn,
)
from rich.table import Table
from rich.prompt import Prompt
from rich.panel import Panel

# ===== SETTINGS =====
SERVER_URL = os.getenv("FITGIRL_SERVER_URL", "http://62.84.179.0:7284")
DOWNLOAD_PATH = Path(os.getenv("FITGIRL_LOCAL_PATH", "./GAMES"))
CHUNK_SIZE = 1024 * 1024  # 1MB

console = Console()
DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)


def get_short_name(filename: str) -> str:
    part_match = re.search(r'(part\d+)', filename, re.IGNORECASE)
    return part_match.group(1) if part_match else filename[:20]


def format_size(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


class FitGirlClient:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def start_download(self, url: str) -> dict:
        async with self.session.post(f"{SERVER_URL}/start", json={"url": url}) as resp:
            if resp.status != 200:
                raise Exception(f"Failed: {await resp.text()}")
            return await resp.json()

    async def get_games(self) -> dict:
        async with self.session.get(f"{SERVER_URL}/games") as resp:
            return await resp.json()

    async def get_status(self, game_id: str) -> dict:
        async with self.session.get(f"{SERVER_URL}/status/{game_id}") as resp:
            return await resp.json()

    async def get_files(self, game_id: str) -> dict:
        async with self.session.get(f"{SERVER_URL}/files/{game_id}") as resp:
            return await resp.json()

    async def delete_game(self, game_id: str) -> dict:
        async with self.session.delete(f"{SERVER_URL}/games/{game_id}") as resp:
            return await resp.json()

    async def download_file_from_server(self, game_id, filename, output_path, file_size, progress, task_id):
        """Download file from server with resume support"""
        url = f"{SERVER_URL}/download/{game_id}/{filename}"
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")

        progress.update(task_id, total=file_size)

        # Resume support
        downloaded = 0
        headers = {}
        if temp_path.exists():
            downloaded = temp_path.stat().st_size
            if 0 < downloaded < file_size:
                headers['Range'] = f'bytes={downloaded}-'
                progress.update(task_id, completed=downloaded)

        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 416:
                    downloaded = temp_path.stat().st_size if temp_path.exists() else 0
                elif resp.status not in [200, 206]:
                    raise Exception(f"HTTP {resp.status}")
                else:
                    mode = 'ab' if resp.status == 206 else 'wb'
                    with open(temp_path, mode) as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress.update(task_id, completed=downloaded)

            # Rename to final
            temp_path.rename(output_path)

        except Exception as e:
            console.print(f"[red]Error: {get_short_name(filename)}: {e}[/red]")
            raise

    async def monitor_and_download(self, game_id: str):
        """Monitor server + download ready files simultaneously"""
        console.print(f"\n[cyan]Game: {game_id}[/cyan]")

        # Wait for game info to load
        game_name = None
        while not game_name or game_name == "Loading...":
            try:
                status = await self.get_status(game_id)
                game_name = status.get("name", "Loading...")
            except Exception:
                pass
            if not game_name or game_name == "Loading...":
                await asyncio.sleep(1)

        # Setup destination
        game_path = DOWNLOAD_PATH / game_name
        game_path.mkdir(parents=True, exist_ok=True)
        console.print(f"[cyan]Destination: {game_path}[/cyan]")

        console.print(Panel.fit(
            "[bold yellow]SERVER DL → CLIENT DL (parallel)[/bold yellow]",
            border_style="yellow"
        ))

        # Track state per file:
        # "server_dl" = downloading on server
        # "server_ready" = ready on server, waiting to download to client
        # "client_dl" = downloading to client
        # "done" = downloaded to client
        # "error" = failed
        file_states = {}       # filename -> state
        file_tasks = {}        # filename -> progress task_id
        files_downloading = set()  # files currently downloading to client
        files_done = set()     # files done downloading to client
        MAX_CLIENT_DL = 1      # download 1 file at a time to client

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(30),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=4
        ) as progress:

            while True:
                # 1. Poll server status
                try:
                    server_status = await self.get_status(game_id)
                except Exception:
                    await asyncio.sleep(2)
                    continue

                if server_status["status"] == "error" and not files_downloading:
                    console.print(f"\n[red]Error: {server_status.get('error')}[/red]")
                    return

                # 2. Update progress bars from server status
                for filename, fs in server_status.get("files_status", {}).items():
                    short = get_short_name(filename)
                    total = fs.get("total_size", 0)
                    downloaded = fs.get("downloaded_size", 0)
                    srv_state = fs.get("status", "pending")

                    # Create progress task if new
                    if filename not in file_tasks:
                        file_tasks[filename] = progress.add_task(
                            f"[dim]⋯ {short} [Pending]", total=total or 0
                        )
                        file_states[filename] = "pending"

                    tid = file_tasks[filename]

                    if total > 0:
                        progress.update(tid, total=total)

                    # Only update display if we're not already downloading to client
                    if file_states[filename] in ("pending", "server_dl", "server_ready"):
                        if srv_state == "ready":
                            if file_states[filename] != "server_ready":
                                file_states[filename] = "server_ready"
                                # Check if already downloaded
                                output_path = game_path / filename
                                if output_path.exists() and total > 0 and output_path.stat().st_size == total:
                                    file_states[filename] = "done"
                                    files_done.add(filename)
                                    progress.update(tid, completed=total,
                                                   description=f"[green]✓ {short} [Done]")
                                else:
                                    progress.update(tid, completed=total,
                                                   description=f"[cyan]✓ {short} [Queued]")
                        elif srv_state == "downloading":
                            file_states[filename] = "server_dl"
                            progress.update(tid, completed=downloaded,
                                           description=f"[yellow]↓ {short} [Server]")
                        elif srv_state == "error":
                            file_states[filename] = "error"
                            progress.update(tid, description=f"[red]✗ {short} [Error]")
                        else:
                            progress.update(tid, completed=0,
                                           description=f"[dim]⋯ {short} [Pending]")

                # 3. Start downloading ready files to client
                if len(files_downloading) < MAX_CLIENT_DL:
                    for filename, state in file_states.items():
                        if state == "server_ready" and filename not in files_downloading:
                            # Start downloading this file to client
                            files_downloading.add(filename)
                            file_states[filename] = "client_dl"
                            asyncio.create_task(
                                self._download_to_client(
                                    game_id, filename, game_path, progress,
                                    file_tasks[filename], file_states,
                                    files_downloading, files_done
                                )
                            )
                            break  # Start one at a time per loop

                # 4. Check if everything is done
                all_server_done = server_status["status"] in ("ready", "error")
                all_files_accounted = all(
                    s in ("done", "error") for s in file_states.values()
                ) if file_states else False

                if all_server_done and all_files_accounted and not files_downloading:
                    break

                await asyncio.sleep(1)

        console.print("\n[green]All downloads complete![/green]")

        # RAR extraction
        if Prompt.ask("\nExtract RAR files?", choices=["y", "n"], default="y") == "y":
            console.print(Panel.fit(
                "[bold green]EXTRACTING RAR[/bold green]",
                border_style="green"
            ))
            await self.extract_rar(game_path)

    async def _download_to_client(self, game_id, filename, game_path, progress, tid,
                                   file_states, files_downloading, files_done):
        """Background task: download a single file from server to client"""
        short = get_short_name(filename)
        output_path = game_path / filename

        try:
            # Get file size from server
            files_info = await self.get_files(game_id)
            file_size = 0
            for f in files_info["files"]:
                if f["filename"] == filename:
                    file_size = f["file_size"]
                    break

            progress.update(tid, total=file_size, completed=0,
                           description=f"[cyan]↓ {short} [Client DL]")

            await self.download_file_from_server(
                game_id, filename, output_path, file_size, progress, tid
            )

            file_states[filename] = "done"
            files_done.add(filename)
            progress.update(tid, description=f"[green]✓ {short} [Done]")

        except Exception as e:
            file_states[filename] = "error"
            progress.update(tid, description=f"[red]✗ {short} [Failed]")

        finally:
            files_downloading.discard(filename)

    async def extract_rar(self, game_path: Path):
        try:
            import rarfile
        except ImportError:
            console.print("[red]pip install rarfile + sudo apt install unrar[/red]")
            return

        rar_files = sorted(game_path.glob("*.rar"))
        if not rar_files:
            console.print("[yellow]No RAR files found[/yellow]")
            return

        main_rar = rar_files[0]
        extract_path = game_path / "extracted"
        extract_path.mkdir(exist_ok=True)

        console.print(f"[cyan]Extracting: {main_rar.name}[/cyan]")

        try:
            with rarfile.RarFile(main_rar) as rf:
                with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                              BarColumn(), console=console) as progress:
                    members = rf.namelist()
                    task = progress.add_task("Extracting...", total=len(members))
                    for member in members:
                        rf.extract(member, extract_path)
                        progress.update(task, advance=1)

            console.print(f"[green]Extracted to: {extract_path}[/green]")

            if Prompt.ask("Delete RAR files?", choices=["y", "n"], default="n") == "y":
                for rar in game_path.glob("*.r??"):
                    rar.unlink()
                for rar in game_path.glob("*.rar"):
                    rar.unlink()
                console.print("[green]RAR files deleted[/green]")
        except Exception as e:
            console.print(f"[red]Extraction error: {e}[/red]")


async def show_games(client: FitGirlClient):
    games = await client.get_games()
    if not games["games"]:
        console.print("[yellow]No games[/yellow]")
        return

    table = Table(title="Games")
    table.add_column("ID", style="cyan")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Files", justify="right")

    colors = {"pending": "yellow", "downloading": "blue", "ready": "green", "error": "red"}
    for g in games["games"]:
        c = colors.get(g["status"], "white")
        table.add_row(g["game_id"], g["name"][:40], f"[{c}]{g['status']}[/{c}]", str(g["files_count"]))

    console.print(table)


async def main_menu():
    console.print(Panel.fit("[bold cyan]FitGirl Download Client[/bold cyan]", title="Welcome"))

    async with FitGirlClient() as client:
        while True:
            console.print("\n[bold]Options:[/bold]")
            console.print("1. Download new game")
            console.print("2. Show games")
            console.print("3. Continue download")
            console.print("4. Delete game")
            console.print("5. Exit")

            choice = Prompt.ask("Choose", choices=["1", "2", "3", "4", "5"])

            try:
                if choice == "1":
                    url = Prompt.ask("Enter FitGirl game page URL")
                    if url:
                        result = await client.start_download(url)
                        console.print(f"[green]Started: {result['game_id']}[/green]")
                        await client.monitor_and_download(result["game_id"])
                elif choice == "2":
                    await show_games(client)
                elif choice == "3":
                    await show_games(client)
                    game_id = Prompt.ask("Enter game ID")
                    if game_id:
                        await client.monitor_and_download(game_id)
                elif choice == "4":
                    await show_games(client)
                    game_id = Prompt.ask("Enter game ID to delete")
                    if game_id and Prompt.ask(f"Delete {game_id}?", choices=["y", "n"]) == "y":
                        await client.delete_game(game_id)
                        console.print("[green]Deleted[/green]")
                elif choice == "5":
                    break
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")


if __name__ == "__main__":
    try:
        asyncio.run(main_menu())
    except KeyboardInterrupt:
        console.print("\n[yellow]Cancelled[/yellow]")
