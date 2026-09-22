// Cloudflare / Turnstile bypass helper, driven as a Deno subprocess.
//
// Usage:
//   deno run --allow-all deno/bypass.ts <mirror-url> [--timeout=60] [--referer=<url>]
//
// It launches a real headless Chrome, lets the page's JS run (challenge solving,
// countdowns, form submits, window.open() diverts) and then prints a single JSON
// line to stdout:
//   {"ok":true,"url":"https://...","cookies":[{name,value,domain,path,...},...]}
//   {"ok":false,"reason":"..."}
// The Python side (hosts.py) parses that, returns the direct URL and/or seeds the
// cookies into its aiohttp session so the actual file download stays in Python.

import puppeteer from "npm:puppeteer-core@^23.11.1";

type Cookie = {
  name: string;
  value: string;
  domain: string;
  path: string;
  httpOnly: boolean;
  secure: boolean;
  sameSite?: string;
  expires?: number;
};

type Result = { ok: true; url: string; cookies: Cookie[] } | {
  ok: false;
  reason: string;
};

const CHALK_HINTS = [
  "cf-turnstile",
  "challenge-platform",
  "cf-browser-verification",
  "just a moment",
  "cf_chl",
];

const FILE_EXT_RE = /\.(rar|zip|exe|iso|7z|bin|001|002|003|md5|sfv|apk)$/i;
const PATH_HINT_RE = /(\/dl\/|\/download\/|\/d\/)/i;
const HOST_HINT_RE = /(dlproxy|cftunnel|tunnel|cdn|download|mega\.nz|mediafire|boosty)/i;

function looksFileish(u: string): boolean {
  try {
    const p = new URL(u, "https://x/");
    if (p.protocol !== "http:" && p.protocol !== "https:") return false;
    if (PATH_HINT_RE.test(p.pathname)) return true;
    if (FILE_EXT_RE.test(p.pathname)) return true;
    if (HOST_HINT_RE.test(p.host)) return true;
    return false;
  } catch {
    return false;
  }
}

function hasChallengeMarkers(low: string): boolean {
  return CHALK_HINTS.some((h) => low.includes(h));
}

function findChrome(): string {
  const envPath = Deno.env.get("CHROME_PATH");
  if (envPath) {
    try {
      if (Deno.statSync(envPath).isFile) return envPath;
    } catch { /* fall through */ }
  }

  // scan <project>/chrome for a downloaded chrome binary
  const here = import.meta.dirname ?? ".";
  const projectRoot = new URL("../", new URL(here + "/", "file://")).pathname;
  const candidates: string[] = [];
  const walkToChrome = (dir: string, depth: number) => {
    if (depth > 3) return;
    let entries: Deno.DirEntry[];
    try {
      entries = [...Deno.readDirSync(dir)];
    } catch {
      return;
    }
    for (const e of entries) {
      const full = `${dir}/${e.name}`;
      if (e.isDirectory) {
        candidates.push(`${full}/chrome`);
        if (depth < 3) walkToChrome(full, depth + 1);
      } else if (e.isFile && (e.name === "chrome" || e.name === "chrome-headless-shell")) {
        candidates.push(full);
      }
    }
  };
  walkToChrome(projectRoot.replace(/\/+$/, ""), 0);
  for (const c of candidates) {
    try {
      if (Deno.statSync(c).isFile) return c;
    } catch { /* keep looking */ }
  }

  const which = Deno.Command.sync("which", {
    args: ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser",
      "chrome", "chrome-headless-shell"],
  });
  const found = which.stdout && new TextDecoder().decode(which.stdout).trim()
    .split("\n").map((s) => s.trim()).find(Boolean);
  if (found) return found;
  throw new Error("Chrome binary not found. Run: deno run -A npm:@puppeteer/browsers install chrome@stable");
}

function parseArgs(argv: string[]) {
  const args = { url: "", timeout: 60, referer: "" };
  for (const a of argv) {
    const [k, v] = a.split("=");
    if (k === "--timeout") args.timeout = Number(v) || 60;
    else if (k === "--referer") args.referer = v;
    else if (a.startsWith("http")) args.url = a;
  }
  return args;
}

function serializeCookies(cookies: import("npm:puppeteer-core@^23.11.1").Cookie[]): Cookie[] {
  return cookies.map((c) => ({
    name: c.name,
    value: c.value,
    domain: c.domain,
    path: c.path,
    httpOnly: c.httpOnly,
    secure: c.secure,
    sameSite: (c as { sameSite?: string }).sameSite as Cookie["sameSite"],
    expires: c.expires ?? undefined,
  }));
}

function pickBest(links: Set<string>): string {
  let best = "";
  let bestScore = -1;
  for (const u of links) {
    let score = 0;
    if (PATH_HINT_RE.test(u)) score += 3;
    if (FILE_EXT_RE.test(u)) score += 3;
    if (/dlproxy|tunnel/i.test(u)) score += 2;
    if (HOST_HINT_RE.test(new URL(u).host)) score += 1;
    if (u.includes("datanodes.to")) score -= 2;
    if (score > bestScore) {
      bestScore = score;
      best = u;
    }
  }
  return best;
}

(async () => {
  const { url, timeout, referer } = parseArgs(Deno.args);
  if (!url) {
    console.log(JSON.stringify({ ok: false, reason: "no url argument" }));
    Deno.exit(1);
  }

  const executablePath = findChrome();
  const userDataDir = `/tmp/deno-bypass-${Math.random().toString(36).slice(2)}`;
  const ua =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

  let browser;
  let page;
  const directLinks = new Set<string>();
  let progresses = "";

  try {
    browser = await puppeteer.launch({
      executablePath,
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-features=AutomationControlled",
        "--lang=en-US",
        "--window-size=1280,900",
        `--user-data-dir=${userDataDir}`,
      ],
    });
    page = await browser.newPage();
    await page.setUserAgent(ua);
    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    });
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => undefined });
      Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
      Object.defineProperty(navigator, "plugins", {
        get: () => [{ name: "Chrome PDF Viewer" } as Plugin],
      });
      if (!window.chrome) {
        (window as Record<string, unknown>).chrome = { runtime: {} };
      }
    });
    await page.bringToFront();

    // send a natural Referer by visiting the source page first instead of a
    // manual header (a manual cross-origin Referer header gets us NS_BLOCKED).
    if (referer) {
      try {
        await page.goto(referer, { waitUntil: "domcontentloaded", timeout: 20000 });
      } catch { /* the mirror page itself still opens below */ }
    }

    // capture window.open() diverts (FuckingFast opens .../dl/... in a new tab)
    browser.on("targetcreated", (target) => {
      const t = target.url();
      if (looksFileish(t)) directLinks.add(t);
    });

    // capture 30x redirects that point at a real file (DataNodes final hop)
    page.on("response", (res) => {
      try {
        const status = res.status();
        if (status >= 300 && status < 400) {
          const loc = res.headers()["location"] || res.headers()["Location"];
          if (loc && looksFileish(loc)) {
            directLinks.add(new URL(loc, res.url()).href);
          }
        }
      } catch { /* ignore */ }
    });

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeout * 1000 });

    const deadline = Date.now() + timeout * 1000;
    let clickedOp1 = false;
    let clickedOp2 = false;
    let challenged = false;

    while (Date.now() < deadline && !directLinks.size) {
      const html = await page.content();
      const low = html.toLowerCase();
      challenged = hasChallengeMarkers(low) && !low.includes("downloadForm");

      const hrefs = await page.evaluate(
        () => [...document.querySelectorAll("a[href]")].map((a) => a.href),
      );
      for (const h of hrefs) if (looksFileish(h)) directLinks.add(h);

      // DataNodes flow: walk the free-download forms, click turnstile checkbox.
      const stepped = await page.evaluate(
        ({ op1, op2 }) => {
          const bodyHtml = document.body ? document.body.innerHTML : "";
          const form = (document.getElementById("downloadForm") ||
            document.forms.namedItem("F1")) as HTMLFormElement | null;
          if (form && !op1) {
            const btn = (form.querySelector("button") ||
              form.querySelector("input[type=submit]")) as HTMLElement | null;
            btn?.click();
            return "op1";
          }
          if ((document.querySelector(".btn-blue") ||
              document.querySelector("a[id*=download]") ||
              /download2/i.test(bodyHtml)) && op1 && !op2) {
            const a = document.querySelector(".btn-blue") ||
              document.querySelector("a[id*=download]") ||
              document.querySelector("#download_form a, form[action*=op] button") as HTMLElement;
            if (a) {
              (a as HTMLElement).click();
              return "op2";
            }
          }
          return null;
        },
        { op1: clickedOp1, op2: clickedOp2 },
      );
      if (stepped === "op1") clickedOp1 = true;
      if (stepped === "op2") clickedOp2 = true;

      // try to tick an auto-solving turnstile checkbox if it appeared
      const turn = page.frames().find((f) => f.url().includes("turnstile"));
      if (turn) {
        try {
          await turn.waitForSelector("input[type=checkbox]", { timeout: 800 });
          await turn.click("input[type=checkbox]");
        } catch { /* already solved or interactive-only */ }
      }

      // wait out any visible countdown
      await new Promise((r) => setTimeout(r, 1200));
    }

    const cookies = serializeCookies(await page.cookies());

    if (directLinks.size) {
      const best = pickBest(directLinks);
      console.log(JSON.stringify({ ok: true, url: best, cookies }));
    } else {
      console.log(JSON.stringify({
        ok: false,
        reason: challenged
          ? "Cloudflare challenge kept serving after automated browser wait"
          : "no direct link appeared after automated browser wait",
      }));
    }
  } catch (e) {
    console.log(JSON.stringify({ ok: false, reason: String((e as Error).message).slice(0, 300) }));
  } finally {
    try { progresses && console.error(progresses); } catch { /* ignore */ }
    try { await browser?.close(); } catch { /* ignore */ }
  }
  Deno.exit(0);
})();