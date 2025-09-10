import asyncio
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlencode

from tqdm.auto import tqdm
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError, Browser, BrowserContext, Page, APIResponse

# =========================
# CONFIG
# =========================
INPUT_FILE = "applications.txt"            # one EP app per line
OUT_DIR = Path("epo_register_pdfs")
PROGRESS_FILE = Path("progress.json")
FAIL_LOG = Path("failures.csv")

HEADLESS = False
LANG = "en"
PAGE_TIMEOUT_MS = 40_000
RETRY_ATTEMPTS = 2
RATE_DELAY = 0.2

MAX_WORKERS = 4
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# ---- Filter: only these doc types will be downloaded (regex, case-insensitive)
FILTER_DOC_TYPES = [
    r"\bDecision\s+revok(ing|ation)\s+the\s+European\s+patent\b",
    r"\bRequest\s+for\s+revok(ing|ation)\s+of\s+patent\b",
]

# Download mode
BULK_ZIP = True          # True = post selected IDs to /download to get a ZIP; False = individual PDFs
ZIP_CHUNK_SIZE = 100     # not critical since we’re selecting few docs

# =========================
# Locks
# =========================
progress_lock = asyncio.Lock()
fail_lock = asyncio.Lock()

# =========================
# Helpers
# =========================
def normalize_app(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", s).upper()

def ep_visible_name(s: str) -> str:
    n = normalize_app(s)
    return n if n.startswith("EP") else f"EP{n}"

def load_apps(path: Path) -> List[str]:
    if not path.exists():
        print(f"Input file not found: {path}", file=sys.stderr)
        sys.exit(1)
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]

def load_progress() -> Dict[str, Dict]:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

async def save_progress(progress: Dict[str, Dict]) -> None:
    async with progress_lock:
        PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding="utf-8")

async def append_failure_rows(rows: List[Dict]):
    if not rows:
        return
    async with fail_lock:
        write_header = not FAIL_LOG.exists()
        with FAIL_LOG.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "app", "level", "context", "detail"])
            if write_header:
                writer.writeheader()
            for r in rows:
                writer.writerow(r)

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def app_doclist_url(app: str) -> str:
    return f"https://register.epo.org/application?number={ep_visible_name(app)}&lng={LANG}&tab=doclist"

# =========================
# Page helpers
# =========================
async def accept_cookies_if_present(page: Page):
    try:
        btn = page.locator("#onetrust-accept-btn-handler")
        if await btn.count():
            await btn.click(timeout=5_000)
            await asyncio.sleep(0.2)
            return
    except Exception:
        pass
    try:
        await page.get_by_role("button", name=re.compile("Accept all cookies", re.I)).click(timeout=3_000)
        await asyncio.sleep(0.2)
    except Exception:
        pass

async def open_doclist(page: Page, app: str):
    await page.goto("https://register.epo.org/", timeout=PAGE_TIMEOUT_MS)
    await accept_cookies_if_present(page)
    await page.goto(app_doclist_url(app), timeout=PAGE_TIMEOUT_MS)
    await accept_cookies_if_present(page)

    try:
        await page.wait_for_selector('table.docList', timeout=10_000)
    except PWTimeoutError:
        tab = page.locator('a[href*="tab=doclist"]')
        if await tab.count():
            await tab.first.click()
        await page.wait_for_selector('table.docList', timeout=20_000)

    # If there’s any Show more/More button, click until finished
    while True:
        clicked = False
        for sel in [
            'button:has-text("Show more")', 'a:has-text("Show more")',
            'button:has-text("More")', 'a:has-text("More")',
            'button:has-text("Load more")', 'a:has-text("Load more")',
        ]:
            el = page.locator(sel)
            if await el.count():
                try:
                    await el.first.click(timeout=2_000)
                    await asyncio.sleep(0.25)
                    clicked = True
                    break
                except Exception:
                    pass
        if not clicked:
            break

async def collect_filtered(page: Page, app: str) -> List[Tuple[str, str]]:
    """
    Return [(doc_id, type_text)] filtered by FILTER_DOC_TYPES.
    - doc_id from: input[name=identivier][value=…]
    - type_text from the link text in 3rd column (the <a> inside that row)
    """
    await open_doclist(page, app)

    rows = await page.locator("table.docList tbody tr").all()
    out: List[Tuple[str, str]] = []

    # Compile patterns once
    pats = [re.compile(p, re.I) for p in FILTER_DOC_TYPES]

    for row in rows:
        # doc id
        cb = row.locator('input[name="identivier"]')
        if await cb.count() == 0:
            continue
        doc_id = await cb.first.get_attribute("value")
        if not doc_id:
            continue

        # doc type text = anchor text in 3rd column
        a = row.locator("td:nth-child(3) a")
        doc_text = (await a.text_content() or "").strip()

        if any(p.search(doc_text) for p in pats):
            out.append((doc_id, doc_text))

    # de-dupe by doc_id preserving order
    seen, filt = set(), []
    for did, txt in out:
        if did not in seen:
            filt.append((did, txt))
            seen.add(did)
    return filt

# =========================
# Downloaders
# =========================
async def download_pdf(context: BrowserContext, ep: str, doc_id: str, out_path: Path) -> bool:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base = "https://register.epo.org/application"
    url = f"{base}?{urlencode({'documentId': doc_id, 'number': ep_visible_name(ep), 'lng': LANG, 'npl': 'false'})}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp: APIResponse = await context.request.get(url, timeout=30_000)
            if resp.ok:
                data = await resp.body()
                if data:
                    out_path.write_bytes(data)
                    return True
        except Exception:
            pass
        await asyncio.sleep(0.5 * attempt)
    return False

async def download_zip_selected(context: BrowserContext, ep: str, doc_ids: List[str], out_zip: Path) -> bool:
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    url = "https://register.epo.org/download"
    payload = {
        "documentIdentifiers": "+".join(doc_ids),   # same as the page’s form
        "number": ep_visible_name(ep),
        "unip": "false",
        "output": "zip",
    }
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp: APIResponse = await context.request.post(url, form=payload, timeout=60_000)
            if resp.ok:
                data = await resp.body()
                if data:
                    out_zip.write_bytes(data)
                    return True
        except Exception:
            pass
        await asyncio.sleep(0.8 * attempt)
    return False

# =========================
# Per-application
# =========================
async def process_app(context: BrowserContext, app: str, progress: Dict[str, Dict],
                      filtered: List[Tuple[str, str]] | None = None,
                      app_pbar: tqdm | None = None) -> Tuple[int, List[Dict]]:
    failures: List[Dict] = []
    norm = normalize_app(app)
    app_vis = ep_visible_name(app)
    app_dir = OUT_DIR / app_vis
    app_dir.mkdir(parents=True, exist_ok=True)

    # resume info
    async with progress_lock:
        app_state = progress.get(norm, {"downloaded_docIds": []})
        downloaded_docids = set(app_state.get("downloaded_docIds", []))

    page = await context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT_MS)
    try:
        if filtered is None:
            filtered = await collect_filtered(page, app)
    except Exception as e:
        failures.append({"timestamp": ts(), "app": app_vis, "level": "APP",
                         "context": "collect_filtered", "detail": repr(e)})
        await page.close()
        return 0, failures
    await page.close()

    if app_pbar:
        app_pbar.total = len(filtered)
        app_pbar.refresh()

    if not filtered:
        # nothing to do
        if app_pbar:
            app_pbar.close()
        return 0, failures

    # Bulk ZIP path (faster even for a few docs)
    if BULK_ZIP:
        doc_ids = [d for d, _ in filtered]
        zip_name = f"{app_vis}_selected_{len(doc_ids)}.zip"
        out_zip = app_dir / zip_name
        ok = await download_zip_selected(context, app_vis, doc_ids, out_zip)
        if ok and out_zip.exists() and out_zip.stat().st_size > 0:
            async with progress_lock:
                downloaded_docids.update(doc_ids)
                progress[norm] = {"downloaded_docIds": sorted(downloaded_docids)}
                await save_progress(progress)
            if app_pbar:
                app_pbar.update(len(doc_ids))
            return len(doc_ids), failures
        else:
            failures.append({"timestamp": ts(), "app": app_vis, "level": "FILE",
                             "context": "ZIP selected", "detail": "zip_download_failed"})
            if app_pbar:
                app_pbar.update(len(doc_ids))
            return 0, failures

    # Individual PDFs
    downloaded_count = 0
    for idx, (doc_id, label) in enumerate(filtered, start=1):
        already = doc_id in downloaded_docids and any(
            p.name.endswith(f"_{doc_id}.pdf") for p in app_dir.glob(f"*_{doc_id}.pdf")
        )
        if already:
            if app_pbar: app_pbar.update(1)
            continue

        safe_label = re.sub(r"[^\w\-_. ]+", "_", label)[:80] or "document"
        fname = f"{app_vis}_{idx:03d}_{safe_label}_{doc_id}.pdf"
        out_pdf = app_dir / fname

        ok = await download_pdf(context, app_vis, doc_id, out_pdf)
        if ok and out_pdf.exists() and out_pdf.stat().st_size > 0:
            downloaded_count += 1
            downloaded_docids.add(doc_id)
            async with progress_lock:
                progress[norm] = {"downloaded_docIds": sorted(downloaded_docids)}
                await save_progress(progress)
        else:
            failures.append({"timestamp": ts(), "app": app_vis, "level": "FILE",
                             "context": f"{doc_id} ({label})", "detail": "pdf_download_failed"})

        if app_pbar:
            app_pbar.update(1)
        await asyncio.sleep(RATE_DELAY)

    return downloaded_count, failures

# =========================
# Worker / Orchestration
# =========================
async def worker(name: int, browser: Browser, queue: asyncio.Queue,
                 progress: Dict[str, Dict], global_pbar: tqdm):
    failures_batch: List[Dict] = []
    context = await browser.new_context(
        accept_downloads=True,
        user_agent=USER_AGENT,
        java_script_enabled=True,
        viewport={"width": 1400, "height": 900}
    )
    try:
        while True:
            try:
                app = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if queue.empty():
                    break
                continue

            app_vis = ep_visible_name(app)

            # Collect filtered list first to size the per-app bar
            filtered: List[Tuple[str, str]] = []
            page = await context.new_page()
            page.set_default_timeout(PAGE_TIMEOUT_MS)
            try:
                filtered = await collect_filtered(page, app)
            except Exception as e:
                failures_batch.append({"timestamp": ts(), "app": app_vis, "level": "APP",
                                       "context": "collect_filtered", "detail": repr(e)})
                await page.close()
                global_pbar.update(1)
                queue.task_done()
                continue
            await page.close()

            app_pbar = tqdm(total=len(filtered), desc=f"{app_vis} (docs)",
                            position=name, leave=False, dynamic_ncols=True)

            try:
                downloaded, fails = await process_app(context, app, progress, filtered=filtered, app_pbar=app_pbar)
                failures_batch.extend(fails)
            except Exception as e:
                failures_batch.append({"timestamp": ts(), "app": app_vis, "level": "APP",
                                       "context": "worker_exception", "detail": repr(e)})
            finally:
                app_pbar.close()
                global_pbar.update(1)
                queue.task_done()

            if len(failures_batch) >= 10:
                await append_failure_rows(failures_batch)
                failures_batch.clear()

        if failures_batch:
            await append_failure_rows(failures_batch)
            failures_batch.clear()
    finally:
        await context.close()

async def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    apps = load_apps(Path(INPUT_FILE))
    progress = load_progress()

    queue: asyncio.Queue = asyncio.Queue()
    for app in apps:
        await queue.put(app)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        with tqdm(total=len(apps), desc="Applications", position=0, dynamic_ncols=True) as global_pbar:
            workers = [asyncio.create_task(worker(i + 1, browser, queue, progress, global_pbar))
                       for i in range(MAX_WORKERS)]
            await queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        await browser.close()

    print(f"\nDone. Output under: {OUT_DIR}")
    print(f"Resume state: {PROGRESS_FILE}")
    if FAIL_LOG.exists():
        print(f"Failures recorded in: {FAIL_LOG}")

if __name__ == "__main__":
    asyncio.run(main())
