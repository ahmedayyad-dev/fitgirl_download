"""
Dynamic multi-hoster support, shared by the direct downloader (fitgirl-download.py)
and the VPS intermediary server (server.py).

Mirror discovery is dynamic: every hoster present on a FitGirl page is detected
automatically - there is no fixed "only this host works" hardcoding anymore.

To add a new hoster:
  1. Add its domain(s) to HOST_ALIASES (domain -> hoster key).
  2. Add a resolver in RESOLVERS (key -> async fn(session, url) -> direct url | None).
     If you skip step 2 the generic fallback resolver tries best-effort instead.
"""

import asyncio
import json
import os
import re
import shutil
import time
from urllib.parse import urlsplit

import aiohttp
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
}

# Mirror domains -> hoster key. Multiple domains may map to the same hoster.
HOST_ALIASES = {
    "fuckingfast.co": "fuckingfast",
    "fuckingfast.net": "fuckingfast",
    "fuckingfast.host": "fuckingfast",
    "filekeeper.net": "filekeeper",
    "datanodes.to": "datanodes",
}

HOSTER_LABELS = {
    "fuckingfast": "FuckingFast",
    "filekeeper": "FileKeeper.net",
    "datanodes": "DataNodes",
}

# A brand-new hoster (no alias yet) is still accepted as a mirror when its anchor
# text looks like a real file name.
FILENAME_RE = re.compile(r"\.(rar|exe|zip|iso|7z|bin|001|002|003|md5|sfv)$", re.IGNORECASE)

RESOLVE_CONCURRENCY = 5

# Server auto-picks a mirror in this order (fast, reliable hosters first).
PREFERRED_HOSTS = ["fuckingfast", "filekeeper", "datanodes"]

DIVERT_RE = re.compile(r'window\.open\(["\']([^"\']+/dl/[^"\']+)["\']')

# Optional chrome-fingerprint transport used to retry anti-bot (Cloudflare) pages.
try:
    from curl_cffi.requests import AsyncSession as CurlSession
except Exception:  # curl-cffi not installed -> aiohttp-only
    CurlSession = None


def browser_session():
    """Return a curl-cffi session impersonating Chrome (better TLS fingerprint)."""
    return CurlSession(impersonate="chrome", headers=HEADERS)


def _status(r):
    for attr in ("status_code", "status"):
        v = getattr(r, attr, None)
        if v is not None:
            return v
    return 0


async def _text(r):
    text = getattr(r, "text", None)
    coroutine = text() if callable(text) else text
    return await coroutine if asyncio.iscoroutine(coroutine) else coroutine


def _netloc(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower()
    except ValueError:
        return ""


def host_key(url: str) -> str | None:
    """Map a mirror URL to its hoster key, or None if it isn't a known hoster."""
    return HOST_ALIASES.get(_netloc(url))


def is_mirror_link(url: str, text: str) -> str | None:
    """Return the hoster key (or the domain for unknown hosters) if `url` is a
    mirror download link, otherwise None."""
    host = _netloc(url)
    if not host:
        return None

    key = HOST_ALIASES.get(host)
    if key:
        # .../dl/... links are already direct downloads, not mirror pages.
        return None if "/dl/" in url else key

    # Page navigation, related posts and paste.fitgirl-repacks.site links are not mirrors.
    if "fitgirl-repacks" in host:
        return None

    name = (text or "").strip()
    if name and FILENAME_RE.search(name):
        return host  # unknown hoster -> grouped by its domain
    return None


def extract_mirror_links(soup: BeautifulSoup) -> dict[str, list[dict]]:
    """Dynamically collect every hoster mirror present on a FitGirl page.

    Returns {hoster_key: [{'url', 'name', 'hoster'}, ...]}.
    """
    links_by_host: dict[str, list[dict]] = {}
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        url = a["href"].strip()
        if not url or url in seen:
            continue
        key = is_mirror_link(url, a.get_text(strip=True))
        if not key:
            continue
        seen.add(url)
        links_by_host.setdefault(key, []).append({
            "url": url,
            "name": a.get_text(strip=True),
            "hoster": key,
        })

    return links_by_host


# ===== PER-HOSTER RESOLVERS =====
# Every resolver returns (direct_url | None, reason). reason explains a failure.

ANTI_BOT_HINTS = ("cf-turnstile", "challenge-platform", "cf-browser-verification",
                  "just a moment", "cf_chl_opt")


def _anti_bot_reason(html: str) -> str | None:
    low = html.lower()
    if "cf-turnstile" in low or "challenge-platform" in low:
        return "Cloudflare Turnstile (anti-bot, needs a browser)"
    if "cf-browser-verification" in low or "cf_chl" in low or "just a moment" in low:
        return "Cloudflare challenge (anti-bot, needs a browser)"
    return None


def _is_curl(session) -> bool:
    return CurlSession is not None and isinstance(session, CurlSession)


async def _fetch_any(session, url, *, method="get", **kwargs):
    """Fetch a page with an aiohttp or curl-cffi session.

    Returns (status, html, headers, final_url). Handles both response APIs.
    """
    if "timeout" not in kwargs:
        kwargs["timeout"] = 30 if _is_curl(session) else aiohttp.ClientTimeout(total=30)
    fn = getattr(session, method)
    resp = await fn(url, **kwargs)
    return _status(resp), await _text(resp), dict(resp.headers), str(getattr(resp, "url", url))


async def _with_browser_fallback(url: str, probe) -> tuple[str | None, str]:
    """Try `probe` against the page; if it looks anti-bot, retry with a chrome-fingerprint
    (curl-cffi) session. Returns (direct|None, reason)."""
    try:
        status, html, _h, _u = await probe()
    except Exception as e:
        return None, str(e)

    reason = _anti_bot_reason(html)
    if not reason:
        m = DIVERT_RE.search(html)
        return (m.group(1), "") if m else (None, f"HTTP {status}, no /dl/ link on page")

    if CurlSession is None:
        return None, reason

    try:
        async with browser_session() as s:
            status, html, _h, _u = await _fetch_any(s, url)
        reason2 = _anti_bot_reason(html)
        m = DIVERT_RE.search(html)
        if m:
            return m.group(1), ""
        return None, reason2 or f"still blocked after curl-cffi retry (HTTP {status})"
    except Exception as e:
        return None, f"{reason}; curl-cffi retry failed: {e}"


# ===== DENO BROWSER BYPASS (real headless Chrome runs the page JS) =====
# Only used as a last resort when both aiohttp and curl-cffi hit an anti-bot page.
# One launch per hoster per cooldown window, because Chrome boot is expensive.

_DENO_BIN = os.environ.get("DENO") or shutil.which("deno")
_DENO_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "deno", "bypass.ts")
_BYPASS_COOLDOWN = 900.0  # seconds
_deno_last_try: dict[str, float] = {}


def _deno_try_due(host: str) -> bool:
    if _DENO_BIN is None or not os.path.isfile(_DENO_SCRIPT):
        return False
    now = time.monotonic()
    if now - _deno_last_try.get(host, 0.0) >= _BYPASS_COOLDOWN:
        _deno_last_try[host] = now
        return True
    return False


async def _run_deno_bypass(url: str, referer: str = "") -> dict:
    timeout_sec = 55
    cmd = [_DENO_BIN, "run", "-A", "--quiet", _DENO_SCRIPT, url,
           f"--timeout={timeout_sec}"]
    if referer:
        cmd.append(f"--referer={referer}")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec + 15)
        except asyncio.TimeoutError:
            proc.kill()
            return {}
        lines = (out or b"").decode("utf-8", "replace").strip().splitlines()
        if not lines:
            return {}
        payload = json.loads(lines[-1])
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


async def _seed_cookies(session, cookies: list) -> None:
    """Inject browser cookies (cf_clearance etc.) into an aiohttp cookie jar."""
    from http.cookies import SimpleCookie
    for c in cookies:
        try:
            if not c.get("name") or not c.get("value"):
                continue
            sim = SimpleCookie()
            sim[c["name"]] = c["value"]
            if c.get("domain"):
                sim[c["name"]]["domain"] = c["domain"].lstrip(".")
            if c.get("path"):
                sim[c["name"]]["path"] = c["path"]
            if c.get("secure"):
                sim[c["name"]]["secure"] = True
            if c.get("httpOnly"):
                sim[c["name"]]["httponly"] = True
            if c.get("expires"):
                sim[c["name"]]["expires"] = c["expires"]
            session.cookie_jar.update_cookies(sim)
        except Exception:
            continue


async def _try_deno_bypass(session, url: str, referer: str, retry_fn, host: str):
    """One cooldown-capped deno attempt. Returns (direct_url|None, reason)."""
    if not _deno_try_due(host):
        return None, "deno bypass: rate-limited (cooldown)"

    payload = await _run_deno_bypass(url, referer)
    if not payload:
        return None, "deno bypass: no response from script"

    direct = payload.get("url") if payload.get("ok") else None
    if direct:
        return direct, ""

    cookies = payload.get("cookies") or []
    if cookies:  # clearance cookies: seed the session, then normal resolution works
        try:
            await _seed_cookies(session, cookies)
            out, reason = await retry_fn()
            if out:
                return out, ""
            return None, reason or (payload.get("reason") or "deno: cookies did not unblock")
        except Exception as e:
            return None, f"deno: cookie retry failed ({e})"

    return None, payload.get("reason") or "deno bypass: no direct link or cookies"


async def resolve_fuckingfast(session, url, referer=""):
    """FuckingFast: the page JS window.open()s a .../dl/... direct link."""
    async def probe():
        return await _fetch_any(session, url)
    direct, reason = await _with_browser_fallback(url, probe)
    if direct or not reason.startswith("Cloudflare"):
        return direct, reason
    return await _try_deno_bypass(session, url, referer, probe, "fuckingfast")


async def resolve_filekeeper(session, url, referer=""):
    """FileKeeper: open page -> POST op=download2 -> 302 Location (countdown is JS-only)."""
    try:
        # Dedicated session per file so the file_code cookie doesn't mix between files.
        async with aiohttp.ClientSession(headers=HEADERS) as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    return None, f"HTTP {r.status}"
                html = await r.text()
                page_url = str(r.url)

            soup = BeautifulSoup(html, "html.parser")
            countdown = soup.find(id="download-countdown")
            if not countdown:
                return None, "no #download-countdown element on page"
            code = countdown.get("data-code")
            if not code:
                return None, "empty data-code"
            if countdown.get("data-has-password") == "true" or countdown.get("data-has-captcha") == "true":
                return None, "download protected (password/captcha)"

            data = {
                "op": "download2",
                "id": code,
                "rand": countdown.get("data-rand") or "",
                "referer": countdown.get("data-referer") or "",
                "method_free": countdown.get("data-method") or "Free download",
                "down_direct": "1",
            }
            async with s.post(page_url, data=data, allow_redirects=False,
                              timeout=aiohttp.ClientTimeout(total=30)) as p:
                if p.status == 302:
                    return p.headers.get("Location"), ""
                return None, f"POST returned HTTP {p.status}, no 302 Location"
    except Exception as e:
        return None, str(e)


async def _datanodes_flow(s, url: str) -> tuple[str | None, str]:
    status, html, _h, page_url = await _fetch_any(s, url)
    reason = _anti_bot_reason(html)

    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", id="downloadForm")
    if not form:  # the turnstile widget markup alone is not an obstacle if the form exists
        return None, reason or "no download form on page"

    fields = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if name and name != "fname" and not inp.get("disabled"):
            fields[name] = inp.get("value") or ""
    if "op" not in fields or "id" not in fields:
        return None, "download form has no id/op fields"

    for attempt in ("download1", "download2"):
        data = dict(fields, op=attempt)
        status, _h2, headers, _f = await _fetch_any(s, page_url, method="post",
                                                     data=data, allow_redirects=False)
        if status in (301, 302, 303):
            loc = headers.get("Location") or headers.get("location")
            if loc and "datanodes.to" not in loc:
                return loc, ""
    return None, "no direct link from POST (JS/Turnstile flow)"


async def resolve_datanodes(session, url, referer=""):
    """DataNodes: best-effort. The free download is JS + Turnstile driven, so it
    usually cannot be resolved server-side - the other mirrors take over."""
    try:
        async def flow(s):
            return await _datanodes_flow(s, url)

        result = await flow(session)
        if result[1].startswith("Cloudflare") and CurlSession is not None:
            async with browser_session() as s:  # retry with a chrome fingerprint
                result = await flow(s)
        if result[1].startswith("Cloudflare") or result[1].startswith("Turnstile"):
            return await _try_deno_bypass(session, url, referer, lambda: flow(session), "datanodes")
        return result
    except Exception as e:
        return None, str(e)


async def _generic_probe(s, url: str) -> tuple[str | None, str]:
    status, html, headers, final = await _fetch_any(s, url)
    ctype = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
    if "html" not in ctype and "text" not in ctype:
        return url, ""
    reason = _anti_bot_reason(html)
    if reason:
        return None, reason
    m = DIVERT_RE.search(html)
    if m:
        return m.group(1), ""
    return None, "page is HTML and has no /dl/ link"


async def resolve_generic(session, url, referer=""):
    """Catch-all fallback: the URL may already stream the file (CDN / direct hoster),
    or hide the direct link behind the same window.open() pattern."""
    try:
        try:
            status, _html, headers, final = await _fetch_any(session, url, method="head",
                                                             allow_redirects=True)
            ctype = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
            if "html" not in ctype and "text" not in ctype:
                length = int(headers.get("Content-Length") or headers.get("content-length") or 0)
                if length > 0:
                    return url, ""
        except Exception:
            pass

        result = await _generic_probe(session, url)
        if result[1].startswith("Cloudflare") and CurlSession is not None:
            async with browser_session() as s:
                result = await _generic_probe(s, url)
        if result[1].startswith("Cloudflare"):
            return await _try_deno_bypass(session, url, referer, lambda: _generic_probe(session, url),
                                          "generic")
        return result
    except Exception as e:
        return None, str(e)


RESOLVERS = {
    "fuckingfast": resolve_fuckingfast,
    "filekeeper": resolve_filekeeper,
    "datanodes": resolve_datanodes,
}


# ===== PUBLIC API =====

async def get_direct_link(session, link: dict, referer: str = "") -> tuple[str | None, str]:
    """Resolve a mirror link ({'url','name','hoster'}) to its direct download URL.

    Returns (direct_url | None, reason).
    """
    hoster = link.get("hoster") or host_key(link["url"])
    resolver = RESOLVERS.get(hoster, resolve_generic)
    return await resolver(session, link["url"], referer)


async def resolve_host(session, links: list[dict], host: str | None = None,
                       referer: str = "") -> list[dict]:
    """Resolve every mirror link of one hoster to its direct URL, in parallel."""
    semaphore = asyncio.Semaphore(RESOLVE_CONCURRENCY)

    async def one(link):
        async with semaphore:
            direct, _reason = await get_direct_link(session, link, referer)
            return link, direct

    results = await asyncio.gather(*[one(l) for l in links])

    resolved = []
    for link, direct in results:
        if direct:
            resolved.append({
                "name": link["name"],
                "url": direct,
                "hoster": link.get("hoster") or host,
            })
    return resolved