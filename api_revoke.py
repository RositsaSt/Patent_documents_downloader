import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

# =========================
# CONFIG
# =========================
@dataclass
class Config:
    INPUT_FILE: str = "applications.txt"                  # one EP per line (e.g., EP00900573)
    OUT_DIR: Path = Path("epo_register_pdfs_httpx")
    PROGRESS_FILE: Path = Path("progress.json")
    FAIL_LOG: Path = Path("failures.log")
    DEBUG_DIR: Path = Path("debug_dumps")

    LANG: str = "en"                                      # "en" | "de" | "fr"
    MAX_CONCURRENCY: int = 1                           # be nice to the site
    RETRIES: int = 3
    CONNECT_TIMEOUT: float = 15.0
    READ_TIMEOUT: float = 45.0
    RATE_DELAY: float = 4.0                             # small delay between EPs

    USER_AGENT: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

    FORCE: bool = True                                   # if True, re-download even if already done

CFG = Config()

# Extract EP-like tokens such as "EP00901189", "00901189", "EP 00901189"
EP_TOKEN_RE = re.compile(r"(?:EP)?\s*\d{8,}", re.I)

# ---- Filter (regex, case-insensitive)
FILTER_DOC_TYPES = [
    r"\bDecision\s+revok(ing|ation)\s+the\s+European\s+patent\b",
    r"\bRequest\s+for\s+revok(ing|ation)\s+of\s+patent\b",
]
PATTERNS = [re.compile(p, re.I) for p in FILTER_DOC_TYPES]

BASE = "https://register.epo.org/"

# =========================
# Helpers
# =========================
def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def normalize_app(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", s).upper()

def ep_visible_name(s: str) -> str:
    n = normalize_app(s)
    return n if n.startswith("EP") else f"EP{n}"

def app_doclist_url(app: str) -> str:
    return f"{BASE}application?number={ep_visible_name(app)}&lng={CFG.LANG}&tab=doclist"

def load_apps_txt(path: Path) -> List[str]:
    """
    Reads a .txt that may contain EP numbers separated by commas, spaces,
    semicolons, or newlines (any mix). Returns unique EPs in original order.
    """
    text = path.read_text(encoding="utf-8")
    tokens = EP_TOKEN_RE.findall(text)  # find all EP-like tokens
    apps: List[str] = []
    seen = set()
    for tok in tokens:
        ep = ep_visible_name(tok)  # normalizes to "EP########"
        if ep not in seen:
            seen.add(ep)
            apps.append(ep)
    return apps

def load_progress() -> Dict[str, Dict]:
    if CFG.PROGRESS_FILE.exists():
        try:
            return json.loads(CFG.PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_progress(progress: Dict[str, Dict]) -> None:
    CFG.PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding="utf-8")

def log_fail(app: str, context: str, detail: str):
    CFG.FAIL_LOG.parent.mkdir(parents=True, exist_ok=True)
    with CFG.FAIL_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{ts()} [{app}] {context}: {detail}\n")

def ep_already_done(app_vis: str, progress: Dict[str, Dict]) -> bool:
    state = progress.get(app_vis) or {}
    if state.get("completed") is True and not CFG.FORCE:
        return True
    if not CFG.FORCE:
        app_dir = CFG.OUT_DIR / app_vis
        if app_dir.exists():
            for p in app_dir.glob(f"{app_vis}_selected_*.zip"):
                try:
                    if p.stat().st_size > 0:
                        return True
                except Exception:
                    pass
            # also consider PDFs already there
            pdfs = list(app_dir.glob(f"{app_vis}_*.pdf"))
            if any(p.exists() and p.stat().st_size > 0 for p in pdfs):
                return True
    return False

def ensure_dirs():
    CFG.OUT_DIR.mkdir(parents=True, exist_ok=True)
    CFG.DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def debug_dump(app_vis: str, name: str, content: bytes):
    path = CFG.DEBUG_DIR / f"{app_vis}_{name}.html"
    try:
        path.write_bytes(content)
    except Exception:
        pass

def is_pdf_response(resp: httpx.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "application/pdf" in ctype:
        return True
    if resp.content[:4] == b"%PDF":
        return True
    return False

def sanitize_filename(s: str, limit: int = 80) -> str:
    s = re.sub(r"[^\w\-_. ]+", "_", s)
    return s[:limit] if len(s) > limit else s

# =========================
# HTML parsing
# =========================
def parse_doclist_for_matches(html: str) -> List[Tuple[str, str]]:
    """
    Return [(doc_id, doc_text)] where doc_text matches PATTERNS.
    doc_id is from input[name=identifier|identivier|identivierId] value.
    doc_text is anchor text in the 3rd column.
    """
    # Try lxml first; fallback to html.parser if not installed
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("table.docList")
    if not table:
        return []

    out: List[Tuple[str, str]] = []
    rows = table.select("tbody tr") or []
    for tr in rows:
        doc_id = None
        for name in ["identifier", "identivier", "identivierId"]:
            inp = tr.select_one(f'input[name="{name}"]')
            if inp and inp.get("value"):
                doc_id = inp["value"].strip()
                break
        if not doc_id:
            continue

        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        a = tds[2].find("a")
        doc_text = (a.get_text(strip=True) if a else tds[2].get_text(strip=True)) or ""
        if not doc_text:
            continue

        if any(p.search(doc_text) for p in PATTERNS):
            out.append((doc_id, doc_text))

    # de-dupe by doc_id
    seen = set()
    dedup: List[Tuple[str, str]] = []
    for did, txt in out:
        if did not in seen:
            seen.add(did)
            dedup.append((did, txt))
    return dedup

def extract_iframe_src(html: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    ifr = soup.find("iframe")
    if ifr and ifr.get("src"):
        return ifr["src"]
    return None

# =========================
# Network ops
# =========================
def common_headers() -> Dict[str, str]:
    return {
        "User-Agent": CFG.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Connection": "keep-alive",
    }

def download_headers() -> Dict[str, str]:
    return {
        "User-Agent": CFG.USER_AGENT,
        "Referer": BASE,
        "Origin": BASE.rstrip("/"),
        "Accept": "application/zip,application/octet-stream,*/*",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "Connection": "keep-alive",
    }

def pdf_headers(referer: str) -> Dict[str, str]:
    return {
        "User-Agent": CFG.USER_AGENT,
        "Accept": "application/pdf,application/octet-stream,*/*",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Referer": referer,
        "Connection": "keep-alive",
    }

async def get_doclist_html(client: httpx.AsyncClient, app: str) -> Optional[str]:
    # Warm-up GET to set cookies
    try:
        await client.get(BASE, headers=common_headers(), timeout=CFG.CONNECT_TIMEOUT)
    except Exception:
        pass

    url = app_doclist_url(app)
    for attempt in range(1, CFG.RETRIES + 1):
        try:
            r = await client.get(url, headers=common_headers(), timeout=CFG.READ_TIMEOUT)
            if r.status_code == 200 and ("docList" in r.text or "<table" in r.text):
                return r.text
        except Exception:
            if attempt == CFG.RETRIES:
                return None
        await asyncio.sleep(0.4 * attempt)
    return None

async def post_zip_download(client: httpx.AsyncClient, app_vis: str, doc_ids: List[str]) -> Optional[bytes]:
    """
    POST /download with the same fields the UI uses.
    Try a couple of param variants (number/appnumber, add lng).
    """
    if not doc_ids:
        return None

    forms = [
        {"documentIdentifiers": "+".join(doc_ids), "number": app_vis, "unip": "false", "output": "zip"},
        {"documentIdentifiers": "+".join(doc_ids), "appnumber": app_vis, "unip": "false", "output": "zip"},
        {"documentIdentifiers": "+".join(doc_ids), "number": app_vis, "lng": CFG.LANG, "unip": "false", "output": "zip"},
        {"documentIdentifiers": "+".join(doc_ids), "appnumber": app_vis, "lng": CFG.LANG, "unip": "false", "output": "zip"},
    ]

    for payload in forms:
        for attempt in range(1, CFG.RETRIES + 1):
            try:
                r = await client.post(f"{BASE}download", data=payload, headers=download_headers(), timeout=CFG.READ_TIMEOUT)
                if r.status_code == 200:
                    ctype = (r.headers.get("content-type") or "").lower()
                    if "application/zip" in ctype or "application/octet-stream" in ctype or (r.content and r.content.startswith(b"PK")):
                        return r.content if r.content else None
                    # capture HTML error for debugging
                    if "text/html" in ctype and r.content:
                        debug_dump(app_vis, f"zip_error_{attempt}", r.content)
                else:
                    # unexpected status
                    if r.content:
                        debug_dump(app_vis, f"zip_status_{r.status_code}_attempt{attempt}", r.content)
            except Exception as e:
                if attempt == CFG.RETRIES:
                    pass
            await asyncio.sleep(0.6 * attempt)
    return None

async def get_pdf_via_variants(client: httpx.AsyncClient, app_vis: str, doc_id: str, referer: str) -> Optional[bytes]:
    """
    Try multiple URL variants to fetch a single PDF.
    Also follow viewer page to its iframe src when needed.
    """
    # Variants that often yield raw PDF
    variants = [
        {"params": {"documentId": doc_id, "number": app_vis, "lng": CFG.LANG, "npl": "false"}},
        {"params": {"documentId": doc_id, "appnumber": app_vis, "lng": CFG.LANG, "npl": "false"}},
        {"params": {"documentId": doc_id, "number": app_vis, "lng": CFG.LANG}},
        {"params": {"documentId": doc_id, "appnumber": app_vis, "lng": CFG.LANG}},
    ]
    for v in variants:
        try:
            r = await client.get(f"{BASE}application", params=v["params"], headers=pdf_headers(referer), timeout=CFG.READ_TIMEOUT)
            if r.status_code == 200 and is_pdf_response(r):
                return r.content
            # If HTML viewer page, try to follow its iframe src
            if r.status_code == 200 and (b"<iframe" in r.content or b"Register Plus PDF viewer" in r.content):
                src = extract_iframe_src(r.text)
                if src:
                    # make absolute
                    if src.startswith("/"):
                        url = BASE.rstrip("/") + src
                    elif src.startswith("http"):
                        url = src
                    else:
                        url = f"{BASE}{src.lstrip('/')}"
                    r2 = await client.get(url, headers=pdf_headers(referer), timeout=CFG.READ_TIMEOUT)
                    if r2.status_code == 200 and is_pdf_response(r2):
                        return r2.content
        except Exception:
            pass
    return None

# =========================
# Main per-EP workflow
# =========================
async def process_ep(app: str, progress: Dict[str, Dict], client: httpx.AsyncClient, pbar: tqdm) -> Tuple[str, int, str]:
    """
    Returns (EP, downloaded_count, message)
    """
    app_vis = ep_visible_name(app)
    app_dir = (CFG.OUT_DIR / app_vis)
    app_dir.mkdir(parents=True, exist_ok=True)

    # Skip if already completed (unless FORCE=True)
    if ep_already_done(app_vis, progress):
        pbar.write(f"[{app_vis}] Already downloaded previously; skipping (set FORCE=True to redo).")
        return app_vis, 0, "skipped_complete"

    # resume info
    app_state = progress.get(app_vis, {"downloaded_docIds": []})
    downloaded_docids = set(app_state.get("downloaded_docIds", []))

    # 1) get doclist html
    html = await get_doclist_html(client, app_vis)
    if not html:
        pbar.write(f"[{app_vis}] Could not load doclist.")
        log_fail(app_vis, "doclist", "load_failed")
        return app_vis, 0, "doclist_failed"

    # 2) parse matching docs
    matches = parse_doclist_for_matches(html)
    if not matches:
        pbar.write(f"[{app_vis}] No documents matched filters; skipping.")
        progress[app_vis] = {
            "completed": True,     # nothing to do, but mark handled
            "zip_file": None,
            "downloaded_docIds": sorted(downloaded_docids),
            "doc_count": 0,
            "timestamp": ts(),
        }
        return app_vis, 0, "no_matches"

    needed_pairs = [(d, t) for (d, t) in matches if d not in downloaded_docids]
    if not needed_pairs and not CFG.FORCE:
        pbar.write(f"[{app_vis}] Already have all matching docs; skipping.")
        progress[app_vis] = {
            "completed": True,
            "zip_file": None,
            "downloaded_docIds": sorted(downloaded_docids),
            "doc_count": len(downloaded_docids),
            "timestamp": ts(),
        }
        return app_vis, 0, "up_to_date"

    # 3) try ZIP download
    doc_ids = [d for d, _ in (needed_pairs or matches)]
    content = await post_zip_download(client, app_vis, doc_ids)
    if content:
        zip_path = app_dir / f"{app_vis}_selected_{len(doc_ids)}.zip"
        zip_path.write_bytes(content)
        new_ids = set(downloaded_docids) | set(doc_ids)
        progress[app_vis] = {
            "completed": True,
            "zip_file": zip_path.name,
            "downloaded_docIds": sorted(new_ids),
            "doc_count": len(new_ids),
            "timestamp": ts(),
        }
        pbar.write(f"[{app_vis}] ZIP saved: {zip_path.name} ({len(doc_ids)} docs)")
        await asyncio.sleep(CFG.RATE_DELAY)
        return app_vis, len(doc_ids), "ok"

    # 4) fallback: download individual PDFs
    pbar.write(f"[{app_vis}] ZIP failed; falling back to individual PDFs...")
    referer = app_doclist_url(app_vis)
    ok_count = 0
    new_ids = set(downloaded_docids)

    for idx, (doc_id, label) in enumerate(needed_pairs or matches, start=1):
        if doc_id in new_ids and not CFG.FORCE:
            continue
        try:
            pdf = await get_pdf_via_variants(client, app_vis, doc_id, referer)
            if pdf:
                safe_label = sanitize_filename(label) or "document"
                pdf_path = app_dir / f"{app_vis}_{idx:03d}_{safe_label}_{doc_id}.pdf"
                pdf_path.write_bytes(pdf)
                ok_count += 1
                new_ids.add(doc_id)
            else:
                # save debug when we couldn't get PDF
                debug_dump(app_vis, f"pdf_fail_{doc_id}", b"")
        except Exception as e:
            log_fail(app_vis, f"pdf_{doc_id}", repr(e))

    if ok_count > 0:
        progress[app_vis] = {
            "completed": True,
            "zip_file": None,
            "downloaded_docIds": sorted(new_ids),
            "doc_count": len(new_ids),
            "timestamp": ts(),
        }
        pbar.write(f"[{app_vis}] Downloaded {ok_count} PDF(s) individually.")
        await asyncio.sleep(CFG.RATE_DELAY)
        return app_vis, ok_count, "ok_partial" if ok_count < len(doc_ids) else "ok"

    # if we reach here, everything failed
    pbar.write(f"[{app_vis}] All download attempts failed.")
    log_fail(app_vis, "download", "zip_and_individual_failed")
    return app_vis, 0, "failed"

# =========================
# Runner
# =========================
async def main():
    ensure_dirs()
    apps = load_apps_txt(Path(CFG.INPUT_FILE))
    progress = load_progress()

    # Optional: pre-filter EPs already completed to start faster
    if not CFG.FORCE:
        apps = [ep for ep in apps if not ep_already_done(ep_visible_name(ep), progress)]

    limits = httpx.Limits(max_connections=CFG.MAX_CONCURRENCY, max_keepalive_connections=CFG.MAX_CONCURRENCY)
    timeout = httpx.Timeout(CFG.CONNECT_TIMEOUT, read=CFG.READ_TIMEOUT)
    results = []

    async with httpx.AsyncClient(http2=True, limits=limits, timeout=timeout, follow_redirects=True) as client:
        sem = asyncio.Semaphore(CFG.MAX_CONCURRENCY)

        async def worker(ep: str, pbar: tqdm):
            async with sem:
                try:
                    return await process_ep(ep, progress, client, pbar)
                except Exception as e:
                    log_fail(ep_visible_name(ep), "worker", repr(e))
                    return ep_visible_name(ep), 0, f"error:{e}"

        with tqdm(total=len(apps), desc="Applications", unit="app") as pbar:
            tasks = [worker(ep, pbar) for ep in apps]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                results.append(res)
                pbar.update(1)

    save_progress(progress)

    ok = sum(1 for _, _, msg in results if msg.startswith("ok"))
    print(f"\nDone. Output under: {CFG.OUT_DIR}")
    print(f"Progress state: {CFG.PROGRESS_FILE}")
    print(f"Completed OK (incl. partial): {ok}/{len(apps)}")
    if Path(CFG.FAIL_LOG).exists():
        print(f"Failures logged in: {CFG.FAIL_LOG}")
    if CFG.DEBUG_DIR.exists():
        print(f"Debug HTML dumps (if any): {CFG.DEBUG_DIR}")

if __name__ == "__main__":
    asyncio.run(main())
