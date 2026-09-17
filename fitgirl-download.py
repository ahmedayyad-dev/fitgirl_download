import asyncio, aiohttp, os, re, json
from datetime import datetime
from bs4 import BeautifulSoup
import rarfile
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.table import Table
# pip install aiohttp beautifulsoup4 rarfile rich

DOWNLOAD_PATH = "/media/ahmed/Games/GAMES/GL"
AUTO_EXTRACT = True
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
INFO_FILE = ".download_info.json"

# --- التعديل 1: إضافة الـ Semaphore هنا (3 ملفات توازي) ---
sem = asyncio.Semaphore(3)

rarfile.UNRAR_TOOL = '7z'
console = Console()
info_lock = asyncio.Lock()

def load_info(folder):
    p = os.path.join(folder, INFO_FILE)
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return None

def save_info(folder, data):
    data['updated'] = datetime.now().isoformat()
    with open(os.path.join(folder, INFO_FILE), 'w') as f:
        json.dump(data, f, indent=2)

pending_files = lambda info: list(filter(lambda f: f['status'] != 'done', info['files']))
done_count = lambda info: sum(map(lambda f: f['status'] == 'done', info['files']))

def group_files(links):
    groups = {}
    for link in links:
        name = link['name'].lower()
        if 'optional' in name or 'selective' in name:
            m = re.search(r'(optional|selective)[_-]?([a-z]+)', name)
            key = f"optional-{m.group(2)}" if m else "optional"
        elif 'update' in name or 'setup' in name:
            key = "update"
        elif 'bonus' in name:
            key = "bonus"
        else:
            key = "main"
        groups.setdefault(key, []).append(link)
    return groups

async def save_status(folder, name, status, size=None):
    async with info_lock:
        info = load_info(folder)
        if info: # تأكد أن الملف موجود
            for f in info['files']:
                if f['name'] == name:
                    f['status'] = status
                    if size: f['size'] = size
                    break
            save_info(folder, info)

# --- روابط مباشرة حسب الـ hoster ---
HOSTER_LABELS = {'filekeeper': 'FileKeeper.net', 'fuckingfast': 'FuckingFast'}


def classify_link(href: str):
    if 'filekeeper.net/' in href and '/dl/' not in href:
        return 'filekeeper'
    if 'fuckingfast.co/' in href and '/dl/' not in href:
        return 'fuckingfast'
    return None


async def get_fuckingfast_link(session, url):
    try:
        async with session.get(url) as r:
            m = re.search(r'window\.open\(["\']([^"\']+/dl/[^"\']+)["\']', await r.text())
            return m.group(1) if m else None
    except:
        return None


async def get_filekeeper_link(url):
    """FileKeeper: افتح صفحة الملف → POST op=download2 → خد 302 Location مباشرة (الانتظار شغال على JS بس)"""
    try:
        # جلسة خاصة لكل ملف عشان الـ cookies (file_code) متتخبطش مع ملفات تانية
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    return None
                html = await r.text()
                page_url = str(r.url)

            soup = BeautifulSoup(html, 'html.parser')
            countdown = soup.find(id='download-countdown')
            if not countdown:
                return None

            code = countdown.get('data-code')
            if not code:
                return None
            if countdown.get('data-has-password') == 'true' or countdown.get('data-has-captcha') == 'true':
                return None

            rand = countdown.get('data-rand') or ''
            referer = countdown.get('data-referer') or ''
            method = countdown.get('data-method') or 'Free download'

            data = {
                'op': 'download2',
                'id': code,
                'rand': rand,
                'referer': referer,
                'method_free': method,
                'down_direct': '1',
            }
            async with session.post(page_url, data=data, allow_redirects=False,
                                    timeout=aiohttp.ClientTimeout(total=30)) as p:
                if p.status == 302:
                    return p.headers.get('Location')
            return None
    except Exception:
        return None


async def get_direct_link(session, link):
    if link.get('hoster') == 'filekeeper':
        return await get_filekeeper_link(link['url'])
    return await get_fuckingfast_link(session, link['url'])

# --- التعديل 2: دالة التحميل المعدلة (Sempahore + No Retry + No Delete) ---
async def download(session, item, folder, progress, task_id):
    # استخدام الـ Semaphore عشان نضمن ان 3 بس هما اللي شغالين تحميل فعلي
    async with sem:
        name, url = item['name'], item['url']
        path = os.path.join(folder, name)

        if os.path.exists(path) and item.get('size'):
            # زيادة السماحية لـ 10 ميجا عشان فرق الحجم البسيط
            if abs(os.path.getsize(path) - item['size'] * 1024 * 1024) < 1024 * 1024 * 10:
                progress.update(task_id, total=1, completed=1, description=f"[green]✓ {name}")
                return True

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=None, sock_read=60)) as r:
                if r.status != 200: raise Exception(f"HTTP {r.status}")

                # التأكد إنه مش صفحة HTML
                if 'text' in r.headers.get('Content-Type', ''):
                    raise Exception("Invalid Content-Type")

                total = int(r.headers.get('content-length', 0))
                progress.update(task_id, total=total)

                with open(path, 'wb') as f:
                    async for chunk in r.content.iter_chunked(1024 * 1024):
                        f.write(chunk)
                        progress.update(task_id, advance=len(chunk))

                # التحقق من الحجم بدون حذف الملف
                file_size = os.path.getsize(path)
                if total and file_size < (total * 0.95): # لو أقل من 95% يبقى ناقص
                    # os.remove(path) <-- تم إيقاف الحذف عشان متخسرش اللي حملته
                    raise Exception(f"Size mismatch: {file_size}/{total}")

                progress.update(task_id, description=f"[green]✓ {name}")
                await save_status(folder, name, 'done', file_size / 1024 / 1024)
                return True

        except Exception as e:
            # تم إلغاء الـ Recursive Retry (استدعاء الدالة لنفسها)
            progress.update(task_id, description=f"[red]✗ {name} (Stopped)")
            await save_status(folder, name, 'failed')
            return False

async def download_all(session, files, folder):
    with Progress(SpinnerColumn(), "{task.description}", BarColumn(30), "{task.percentage:>3.0f}%",
                  DownloadColumn(), TransferSpeedColumn(), TimeRemainingColumn(), console=console) as progress:
        # هنا بنعمل Tasks لكل الملفات عادي، بس الدالة download جواها Semaphore فهينظموا نفسهم
        tasks = [(f, progress.add_task(f"[yellow]↓ {f['name']}", total=0)) for f in files]
        results = await asyncio.gather(*[download(session, f, folder, progress, tid) for f, tid in tasks])

    # حساب النتائج بناءً على True/False
    success_count = sum(1 for r in results if r is True)
    fail_count = len(results) - success_count
    return success_count, fail_count

async def main():
    console.print("[bold cyan]FitGirl Downloader[/]\n")

    # Scan for games (نفس الكود بتاعك)
    games = []
    if os.path.isdir(DOWNLOAD_PATH):
        for folder in sorted(os.listdir(DOWNLOAD_PATH)):
            folder_path = os.path.join(DOWNLOAD_PATH, folder)
            if os.path.isdir(folder_path):
                info = load_info(folder_path)
                if info and ('url' in info or 'game_url' in info):
                    info['url'] = info.get('url') or info.get('game_url')
                    done = done_count(info)
                    total = info['total_files']
                    status = "[green]✓[/]" if done == total else f"[yellow]{done}/{total}[/]"
                    games.append({'path': folder_path, 'info': info, 'status': status})

    if games:
        console.print("[cyan]Games:[/]")
        for i, g in enumerate(games, 1):
            console.print(f"  {i}. {g['status']} {g['info']['game'][:50]}")
        console.print()

    inp = console.input("[yellow]Number, URL, or path:[/] ").strip()
    if not inp: return

    if inp.isdigit() and 1 <= int(inp) <= len(games):
        url = games[int(inp) - 1]['info']['url']
    elif os.path.isdir(inp):
        info = load_info(inp)
        if not info or ('url' not in info and 'game_url' not in info):
            return console.print("[red]No saved URL in folder[/]")
        url = info.get('url') or info.get('game_url')
    else:
        url = inp

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        # Fetch page
        try:
            async with session.get(url) as r:
                soup = BeautifulSoup(await r.text(), 'html.parser')
        except Exception as e:
            return console.print(f"[red]Error fetching page: {e}[/]")

        title_tag = soup.find('h1', class_='entry-title')
        if not title_tag: return console.print("[red]Could not find game title[/]")

        game = title_tag.get_text(strip=True)
        folder = os.path.join(DOWNLOAD_PATH, re.sub(r'[<>:"/\\|?*]', '', game)[:50])
        os.makedirs(folder, exist_ok=True)

        # Get all links (حسب كل hoster)
        links_by_host = {}
        for a in soup.find_all('a', href=True):
            host = classify_link(a['href'])
            name = a.get_text(strip=True)
            if host and name:
                links_by_host.setdefault(host, []).append({'url': a['href'], 'name': name, 'hoster': host})

        if not links_by_host: return console.print("[red]No links found[/]")

        # اختر الـ mirror
        host_keys = list(links_by_host.keys())
        if len(host_keys) > 1:
            console.print("\n[cyan]Available mirrors:[/]")
            for i, h in enumerate(host_keys, 1):
                console.print(f"  {i}. [yellow]{HOSTER_LABELS.get(h, h)}[/] ({len(links_by_host[h])} files)")
            pick = console.input("\n[yellow]Choose mirror number: [/]").strip()
            host = host_keys[int(pick) - 1] if pick.isdigit() and 1 <= int(pick) <= len(host_keys) else host_keys[0]
        else:
            host = host_keys[0]
        console.print(f"\n[cyan]Mirror:[/] {HOSTER_LABELS.get(host, host)} ({len(links_by_host[host])} files)")

        all_links = links_by_host[host]

        # Group and select
        groups = group_files(all_links)
        console.print(f"\n[green]{game}[/]\n")

        if len(groups) > 1:
            console.print("[cyan]Available groups:[/]")
            for i, (key, files) in enumerate(groups.items(), 1):
                console.print(f"  {i}. [yellow]{key}[/] ({len(files)} files)")
            console.print(f"\n[blue]Enter numbers (e.g. 1,2,3) or 'all':[/]")
            choice = console.input("[yellow]> [/]").strip().lower()
            if choice == 'all':
                selected = all_links
            else:
                keys = list(groups.keys())
                selected = []
                for num in choice.replace(' ', '').split(','):
                    if num.isdigit() and 1 <= int(num) <= len(keys):
                        selected.extend(groups[keys[int(num) - 1]])
        else:
            selected = all_links

        if not selected: return console.print("[red]No files selected[/]")

        # Check what's already done
        info = load_info(folder)
        done_names = set(f['name'] for f in info['files'] if f['status'] == 'done') if info else set()
        selected = [l for l in selected if l['name'] not in done_names]

        if not selected:
            console.print("[green]All selected files already complete![/]")
        else:
            console.print(f"\n[blue]{len(selected)} files to download ({len(done_names)} already done)[/]")
            console.print("\n[cyan]Extracting direct links...[/]")

            # جلب الروابط المباشرة بالتوازي (لـ FileKeeper كل ملف بجلسة مستقلة للكوكيز)
            resolve_sem = asyncio.Semaphore(5)

            async def resolve_one(link):
                async with resolve_sem:
                    return link, await get_direct_link(session, link)

            results = await asyncio.gather(*[resolve_one(l) for l in selected])

            files = []
            for link, dl in results:
                console.print(f"  [{'green' if dl else 'red'}]{'✓' if dl else '✗'}[/] {link['name']}")
                if dl: files.append({'name': link['name'], 'url': dl, 'page_url': link['url'], 'hoster': host, 'status': 'pending'})

            if not files: return console.print("[red]No direct links[/]")

            # Merge with existing info
            if info:
                info['url'] = url
                existing = {f['name']: f for f in info['files']}
                for f in files:
                    existing[f['name']] = f
                info['files'] = list(existing.values())
                info['total_files'] = len(info['files'])
            else:
                info = {'game': game, 'url': url, 'folder': folder, 'total_files': len(files), 'files': files}
            save_info(folder, info)

            console.print(f"[blue]Folder: {folder}[/]\n")

            # Download loop
            while True:
                pending = pending_files(info)
                if not pending:
                    console.print("[green]All files complete![/]")
                    break

                console.print(f"[cyan]Downloading {len(pending)}/{info['total_files']} files...[/]\n")
                ok, fail = await download_all(session, pending, folder)

                info = load_info(folder)
                t = Table(title="Summary")
                t.add_column("", style="cyan"); t.add_column("Value", style="green")
                t.add_row("Success", str(ok)); t.add_row("Failed", str(fail))
                t.add_row("Progress", f"{done_count(info)}/{info['total_files']} ({done_count(info)/info['total_files']*100:.0f}%)")
                console.print(t)

                if fail:
                    # بما أننا أوقفنا الـ Retry التلقائي، سنسأل المستخدم أو ننتظر ونحاول مرة أخرى لكل الملفات الفاشلة
                    console.print(f"\n[yellow]{fail} failed - retrying batch in 5 seconds...[/]")
                    await asyncio.sleep(5)
                else:
                    break

        # Extract if all done
        if AUTO_EXTRACT:
            info = load_info(folder)
            if info and all(map(lambda f: f['status'] == 'done', info['files'])):
                parts = sorted(filter(lambda f: f.endswith('.rar'), os.listdir(folder)))
                first = next(filter(lambda f: '.part01.rar' in f or '.part1.rar' in f or '.part001.rar' in f, parts), None)
                if first:
                    console.print(f"\n[cyan]Extracting {len(parts)} parts...[/]")
                    try:
                        with rarfile.RarFile(os.path.join(folder, first)) as rf:
                            rf_files, total, done = rf.infolist(), sum(map(lambda x: x.file_size, rf.infolist())), 0
                            with Progress(SpinnerColumn(), "{task.description}", BarColumn(30), "{task.percentage:>3.0f}%", console=console) as p:
                                t1, t2 = p.add_task(f"[blue]Parts 0/{len(parts)}", total=len(parts)), p.add_task("[cyan]...", total=len(rf_files))
                                for i, rf_file in enumerate(rf_files):
                                    rf.extract(rf_file, folder)
                                    done += rf_file.file_size
                                    part = min(int(done / total * len(parts)) + 1, len(parts))
                                    p.update(t1, completed=part, description=f"[blue]Parts {part}/{len(parts)}")
                                    p.update(t2, completed=i + 1, description=f"[cyan]{rf_file.filename}")
                        console.print(f"[green]Extracted {len(rf_files)} files ({total / 1024 ** 3:.1f} GB)[/]")
                        if console.input("Delete RAR files? [y/n]: ").lower() == 'y':
                            list(map(lambda x: os.remove(os.path.join(folder, x)), parts))
                            console.print("[green]Deleted[/]")
                    except Exception as e:
                        console.print(f"[red]Extraction failed: {e}[/]")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        console.print("\n[yellow]Cancelled[/]")
