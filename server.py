"""
Local server: lists a public Drive folder via the Drive API (API key),
downloads files in parallel, packs ZIP/tar.gz, exposes job status for UI progress.

Set GOOGLE_API_KEY (or GDRIVE_API_KEY). Optional: GDRIVE_PARALLEL (thread ceiling),
GDRIVE_MAX_SIMULTANEOUS_DOWNLOADS (default 10, max 20) limits concurrent TLS connections
to Google to avoid drive.usercontent.google.com connect timeouts, GDRIVE_CONNECT_TIMEOUT /
GDRIVE_READ_TIMEOUT, GDRIVE_STREAM_CHUNK_MB.
"""

from __future__ import annotations

from pathlib import Path

_env_file = Path(__file__).resolve().parent / ".env"
try:
    from dotenv import load_dotenv

    load_dotenv(_env_file)
except ImportError:
    pass

import gzip
import hmac
import html
import json
import logging
import os
import random
import re
import secrets
import sys
import tarfile
import tempfile
import threading
import time
import uuid
import zipfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterator, TypeVar

_T = TypeVar("_T")

import requests
from flask import Flask, Response, jsonify, request, send_file
from requests.adapters import HTTPAdapter

app = Flask(__name__, static_folder=None)


class _FlushStderrHandler(logging.StreamHandler):
    """Emit each log line immediately so terminal status is visible during long jobs."""

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


_gd_handler = _FlushStderrHandler(sys.stderr)
_gd_handler.setFormatter(
    logging.Formatter("%(asctime)s [gd-dl] %(message)s", datefmt="%H:%M:%S")
)
log = logging.getLogger("gd-dl")
log.setLevel(logging.INFO)
log.addHandler(_gd_handler)
log.propagate = False

logging.getLogger("werkzeug").setLevel(logging.WARNING)

DRIVE_FILES = "https://www.googleapis.com/drive/v3/files"
UC_EXPORT = "https://drive.google.com/uc"
FOLDER_MAX_DEPTH = 10
MAX_FILES_CAP = 3000
GOOGLE_APPS = "application/vnd.google-apps."
JOB_MAX_AGE_SEC = 7200
def _stream_chunk_bytes() -> int:
    try:
        mb = int(os.environ.get("GDRIVE_STREAM_CHUNK_MB", "16"))
    except ValueError:
        mb = 16
    mb = max(4, min(32, mb))
    return mb * 1024 * 1024


STREAM_CHUNK = _stream_chunk_bytes()
DOWNLOAD_RETRY_ATTEMPTS = 3


def _download_gate_limit() -> int:
    """Max concurrent HTTP downloads to Google (avoids connect storms to drive.usercontent.google.com)."""
    try:
        n = int(os.environ.get("GDRIVE_MAX_SIMULTANEOUS_DOWNLOADS", "10"))
    except ValueError:
        n = 10
    return max(2, min(20, n))


_DL_GATE = _download_gate_limit()
_download_gate = threading.BoundedSemaphore(_DL_GATE)


def _http_timeout() -> tuple[int, int]:
    """(connect seconds, read seconds) — short connect fails fast; long read for big files."""
    try:
        c = int(os.environ.get("GDRIVE_CONNECT_TIMEOUT", "30"))
        r = int(os.environ.get("GDRIVE_READ_TIMEOUT", "600"))
    except ValueError:
        c, r = 30, 600
    c = max(10, min(120, c))
    r = max(120, min(3600, r))
    return (c, r)


HTTP_TIMEOUT = _http_timeout()

UA = "Mozilla/5.0 (compatible; GDDownloader/1.0)"

_session = requests.Session()
_session.headers.update({"User-Agent": UA})

_thread_local = threading.local()


def make_worker_session() -> requests.Session:
    s = requests.Session()
    pool = min(32, max(8, _DL_GATE + 4))
    adapter = HTTPAdapter(
        pool_connections=pool,
        pool_maxsize=pool,
        max_retries=0,
    )
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": UA})
    return s


def get_worker_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = make_worker_session()
    return _thread_local.session

jobs_lock = threading.Lock()
jobs: dict[str, dict] = {}


def api_key() -> str | None:
    return os.environ.get("GOOGLE_API_KEY") or os.environ.get("GDRIVE_API_KEY")


def parallel_cap() -> int:
    """Upper bound from GDRIVE_PARALLEL (default 36, clamped 4–48)."""
    try:
        n = int(os.environ.get("GDRIVE_PARALLEL", "36"))
    except ValueError:
        n = 36
    return max(4, min(48, n))


def dynamic_workers(file_count: int, avg_size_mb: float | None) -> int:
    """More workers when there are many smaller files; fewer when averages are huge."""
    cap = parallel_cap()
    if avg_size_mb is not None and avg_size_mb > 120:
        return min(8, cap)
    if avg_size_mb is not None and avg_size_mb > 50:
        return min(12, cap)
    if file_count < 15:
        return min(10, cap)
    if file_count < 60:
        return min(18, cap)
    if file_count < 250:
        return min(28, cap)
    if file_count < 800:
        return min(36, cap)
    return min(40, cap)


def parallel_workers() -> int:
    """Alias for API / health: reports env ceiling."""
    return parallel_cap()


def extract_folder_id(raw: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None
    m = re.search(r"/drive/folders/([a-zA-Z0-9_-]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"folders/([a-zA-Z0-9_-]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9_-]{25,}", s):
        return s
    return None


def extract_file_id(raw: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None
    if re.fullmatch(r"[a-zA-Z0-9_-]{25,}", s) and "/" not in s:
        return s
    for pat in (
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"/open\?id=([a-zA-Z0-9_-]+)",
        r"[?&]id=([a-zA-Z0-9_-]+)",
        r"/uc\?(?:[^&]*&)*id=([a-zA-Z0-9_-]+)",
    ):
        m = re.search(pat, s)
        if m:
            return m.group(1)
    return None


def sanitize_arc_name(name: str) -> str:
    name = name.replace("\x00", "").replace("\\", "_")
    name = name.replace("/", "_").strip()
    if name in ("", ".", ".."):
        return "_unnamed"
    return name


def list_children(key: str, parent_id: str) -> list[dict]:
    out: list[dict] = []
    page_token: str | None = None
    q = f"'{parent_id}' in parents and trashed = false"
    while True:
        params: dict = {
            "q": q,
            "fields": "nextPageToken, files(id, name, mimeType, size)",
            "key": key,
            "pageSize": 1000,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if page_token:
            params["pageToken"] = page_token
        r = _session.get(DRIVE_FILES, params=params, timeout=60)
        if not r.ok:
            raise RuntimeError(
                f"Drive API list failed ({r.status_code}): {r.text[:500]}"
            )
        data = r.json()
        for f in data.get("files") or []:
            out.append(f)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return out


def collect_files(
    key: str,
    folder_id: str,
    max_depth: int,
) -> list[tuple[str, str, int | None]]:
    """Each row: file_id, path, size_bytes from Drive API (or None)."""
    log.info(
        "Listing folder tree (folder_id=%s, max_depth=%s) via Drive API…",
        folder_id[:12] + "…",
        max_depth,
    )
    results: list[tuple[str, str, int | None]] = []

    def walk(fid: str, prefix: str, depth: int) -> None:
        if len(results) >= MAX_FILES_CAP:
            return
        if depth > max_depth:
            return
        for item in list_children(key, fid):
            if len(results) >= MAX_FILES_CAP:
                break
            name = sanitize_arc_name(item.get("name") or "unnamed")
            mime = item.get("mimeType") or ""
            path = f"{prefix}/{name}" if prefix else name
            if mime == "application/vnd.google-apps.folder":
                walk(item["id"], path, depth + 1)
            elif mime.startswith(GOOGLE_APPS):
                continue
            else:
                raw_sz = item.get("size")
                size_b: int | None = None
                if raw_sz is not None:
                    try:
                        size_b = int(raw_sz)
                    except (TypeError, ValueError):
                        size_b = None
                results.append((item["id"], path, size_b))

    walk(folder_id, "", 0)
    log.info("Found %d file(s) to download (cap=%d).", len(results), MAX_FILES_CAP)
    return results


def assign_unique_arc_names(
    paths: list[tuple[str, str, int | None]],
) -> list[tuple[int, str, str]]:
    """(index, file_id, unique_path_in_archive)."""
    seen: set[str] = set()
    out: list[tuple[int, str, str]] = []
    for idx, (fid, arcname, _sz) in enumerate(paths):
        key_path = arcname.replace("\\", "/")
        base = key_path
        n = 1
        while base in seen:
            stem, dot, ext = key_path.rpartition(".")
            if dot:
                base = f"{stem}_{n}.{ext}"
            else:
                base = f"{key_path}_{n}"
            n += 1
        seen.add(base)
        out.append((idx, fid, base))
    return out


def _confirm_from_cookies(resp: requests.Response) -> str | None:
    for k, v in resp.cookies.items():
        if k.startswith("download_warning"):
            return v
    return None


def _confirm_from_html(html: str) -> str | None:
    m = re.search(r"confirm=([^&\"']+)", html)
    if m:
        return m.group(1)
    m = re.search(r'confirmToken"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1)
    return None


def retry_call(fn: Callable[[], _T], attempts: int = DOWNLOAD_RETRY_ATTEMPTS) -> _T:
    last: Exception | None = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            if i == attempts - 1:
                raise
            delay = 2**i + random.random()
            msg = str(e)
            if len(msg) > 220:
                msg = msg[:217] + "…"
            log.warning("Retry %s/%s after error: %s (sleep %.1fs)", i + 1, attempts, msg, delay)
            time.sleep(delay)
    assert last is not None
    raise last


def open_download_stream(
    file_id: str, session: requests.Session | None = None
) -> requests.Response:
    sess = session if session is not None else _session
    params = {"export": "download", "id": file_id}
    r = sess.get(UC_EXPORT, params=params, stream=True, timeout=HTTP_TIMEOUT)
    if not r.ok:
        r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    token = _confirm_from_cookies(r)
    if token:
        r.close()
        r = sess.get(
            UC_EXPORT,
            params={**params, "confirm": token},
            stream=True,
            timeout=HTTP_TIMEOUT,
        )
        if not r.ok:
            r.raise_for_status()
        return r
    if "text/html" in ctype:
        parts: list[bytes] = []
        total = 0
        for chunk in r.iter_content(chunk_size=65536):
            if not chunk:
                continue
            parts.append(chunk)
            total += len(chunk)
            if total >= 524288 or b"confirm=" in chunk or b"download_warning" in chunk:
                break
        r.close()
        html = b"".join(parts).decode("utf-8", errors="ignore")
        token = _confirm_from_html(html)
        if token:
            r2 = sess.get(
                UC_EXPORT,
                params={**params, "confirm": token},
                stream=True,
                timeout=HTTP_TIMEOUT,
            )
            if not r2.ok:
                r2.raise_for_status()
            return r2
        raise RuntimeError(
            "Google returned HTML instead of the file (large file / scan page). "
            "Try downloading that file alone or reduce folder size."
        )
    return r


def copy_stream_to_writer(
    resp: requests.Response,
    write_fn,
    *,
    chunk_size: int = STREAM_CHUNK,
    on_chunk: Callable[[int], None] | None = None,
) -> None:
    try:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:
                if on_chunk is not None:
                    on_chunk(len(chunk))
                write_fn(chunk)
    finally:
        resp.close()


def download_one_task(
    task: tuple[int, str, str],
    track: Callable[[int], None] | None = None,
) -> tuple[int, str | None, str | None]:
    """Return (index, temp_path or None, error or None). Thread-local Session + retries."""
    idx, fid, base = task
    try:
        with _download_gate:
            sess = get_worker_session()

            def open_stream() -> requests.Response:
                return open_download_stream(fid, session=sess)

            resp: requests.Response = retry_call(open_stream)
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" in ctype:
                resp.close()
                return (idx, None, f"not a binary file: {base}")
            fd, path = tempfile.mkstemp(suffix=".part")
            os.close(fd)
            with open(path, "wb") as out_f:
                copy_stream_to_writer(resp, out_f.write, on_chunk=track)
            return (idx, path, None)
    except Exception as e:
        return (idx, None, f"{base}: {e}")


def download_parallel(
    tasks: list[tuple[int, str, str]],
    workers: int,
    on_done: Callable[[int, str, int], None],
    track: Callable[[int], None] | None = None,
) -> list[tuple[int, str, str]]:
    """
    Download all tasks in parallel. on_done(completed_count, last_base, total).
    Returns sorted list of (idx, tmp_path, base). Raises on first error after cleanup.
    """
    total = len(tasks)
    if total == 0:
        return []
    results: list[tuple[int, str, str]] = []
    done_count = 0
    executor = ThreadPoolExecutor(max_workers=workers)
    future_to_task = {
        executor.submit(download_one_task, t, track): t for t in tasks
    }
    try:
        for fut in as_completed(future_to_task):
            task = future_to_task[fut]
            _idx, fid, base = task
            try:
                idx, tmp, err = fut.result()
            except Exception as e:
                err = str(e)
                idx, tmp = task[0], None
            if err:
                raise RuntimeError(err)
            assert tmp is not None
            results.append((idx, tmp, base))
            done_count += 1
            on_done(done_count, base, total)
    except Exception:
        for fut in future_to_task:
            if fut.done():
                try:
                    _idx, tmp, _err = fut.result()
                    if tmp:
                        try:
                            os.unlink(tmp)
                        except OSError:
                            pass
                except Exception:
                    pass
            else:
                fut.cancel()
        for _i, p, _b in results:
            try:
                os.unlink(p)
            except OSError:
                pass
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True, cancel_futures=False)

    results.sort(key=lambda x: x[0])
    return results


def build_zip_from_disk(
    items: list[tuple[str, str]],
    out_path: str,
    on_packed: Callable[[int, int], None] | None = None,
) -> None:
    """items: (tmp_path, arc_base) in order. STORED = no compression (fast; fine for JPEG/video)."""
    n = len(items)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_STORED) as zf:
        for i, (tmp_path, base) in enumerate(items, start=1):
            zf.write(tmp_path, arcname=base)
            if on_packed:
                on_packed(i, n)


def build_tar_gz_from_disk(
    items: list[tuple[str, str]],
    out_path: str,
    on_packed: Callable[[int, int], None] | None = None,
) -> None:
    n = len(items)
    with open(out_path, "wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w") as tf:
                for i, (tmp_path, base) in enumerate(items, start=1):
                    tf.add(tmp_path, arcname=base, recursive=False)
                    if on_packed:
                        on_packed(i, n)


def job_pct(job: dict) -> float:
    t = max(int(job.get("total") or 1), 1)
    phase = job.get("phase") or ""
    if phase == "queued":
        return 1.0
    if phase == "listing":
        return 2.0
    if phase == "downloading":
        c = int(job.get("completed") or 0)
        return 5.0 + (c / t) * 72.0
    if phase == "packing":
        p = int(job.get("pack_current") or 0)
        return 77.0 + (p / t) * 20.0
    if phase == "done":
        return 100.0
    if phase == "error":
        return 0.0
    return 0.0


def job_detail(job: dict) -> str:
    phase = job.get("phase") or ""
    t = int(job.get("total") or 0)
    label = (job.get("label") or "").strip()
    if phase == "queued":
        return "Starting…"
    if phase == "listing":
        return "Listing folder from Google Drive…"
    if phase == "downloading":
        c = int(job.get("completed") or 0)
        tail = f" — {label}" if label else ""
        return f"Downloading {c} / {t} files ({job.get('workers', '?')} parallel){tail}"
    if phase == "packing":
        p = int(job.get("pack_current") or 0)
        return f"Writing archive {p} / {t}…"
    if phase == "done":
        return "Ready — starting download…"
    if phase == "error":
        return job.get("error") or "Error"
    return ""


def sweep_stale_jobs() -> None:
    now = time.time()
    with jobs_lock:
        dead = [
            jid
            for jid, j in jobs.items()
            if now - float(j.get("created", 0)) > JOB_MAX_AGE_SEC
        ]
        for jid in dead:
            j = jobs.pop(jid, None)
            if j and j.get("result_path"):
                try:
                    os.unlink(j["result_path"])
                except OSError:
                    pass
            # temps should be gone after success; listing error has none


def run_archive_job(
    job_id: str,
    key: str,
    raw: str,
    max_depth: int,
    fmt: str,
    folder_id: str,
) -> None:
    tmp_paths_to_clean: list[str] = []
    t_job = time.time()
    bytes_downloaded = 0
    bytes_lock = threading.Lock()
    _emit_bd = [0, 0.0]

    def track_bytes(n: int) -> None:
        nonlocal bytes_downloaded
        with bytes_lock:
            bytes_downloaded += n
            bd = bytes_downloaded
        now_m = time.monotonic()
        if bd - _emit_bd[0] >= 512 * 1024 or now_m - _emit_bd[1] >= 0.45:
            _emit_bd[0] = bd
            _emit_bd[1] = now_m
            j_update(bytes_downloaded=bd)

    def j_update(**kwargs) -> None:
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id].update(kwargs)

    try:
        log.info(
            "━━ Job %s ━━ folder=%s… format=%s",
            job_id[:14],
            folder_id[:14],
            fmt,
        )
        j_update(phase="listing", label="", workers=parallel_cap())
        paths = collect_files(key, folder_id, max_depth)
        t_list_end = time.time()
        if not paths:
            j_update(phase="error", error="No downloadable files found.")
            log.info("Job %s: no files to download — stopping.", job_id[:14])
            return

        total = len(paths)
        sizes_known = [s for _, _, s in paths if s is not None and s > 0]
        avg_size_mb = (
            (sum(sizes_known) / len(sizes_known)) / (1024 * 1024)
            if sizes_known
            else None
        )
        workers = dynamic_workers(total, avg_size_mb)
        total_bytes = int(
            sum(int(s) for _, _, s in paths if isinstance(s, int) and s > 0)
        )
        sized_files = sum(1 for _, _, s in paths if s)
        log.info(
            "Job %s: LIST done — %d files in %.2fs | workers=%d (cap=%d)%s | ~%s (%d/%d sizes)",
            job_id[:14],
            total,
            t_list_end - t_job,
            workers,
            parallel_cap(),
            f" | avg file ~{avg_size_mb:.1f} MiB" if avg_size_mb is not None else "",
            f"{total_bytes / (1024**3):.2f} GiB" if total_bytes > 0 else "?",
            sized_files,
            total,
        )
        j_update(
            total=total,
            total_bytes=total_bytes,
            sized_files=sized_files,
            bytes_downloaded=0,
            phase="downloading",
            completed=0,
            label="",
            workers=workers,
        )

        tasks = assign_unique_arc_names(paths)

        t_dl_start = time.monotonic()
        last_status_log = [0.0]

        def bytes_so_far() -> int:
            with bytes_lock:
                return bytes_downloaded

        def on_done(done: int, base: str, tot: int) -> None:
            j_update(completed=done, label=base, bytes_downloaded=bytes_so_far())
            now_m = time.monotonic()
            pct = 100.0 * done / max(tot, 1)
            step = max(1, tot // 35)
            interval_ok = (now_m - last_status_log[0]) >= 12.0
            milestone = done in (1, tot) or done % step == 0
            if milestone or interval_ok:
                last_status_log[0] = now_m
                elapsed = now_m - t_dl_start
                mb = bytes_so_far() / (1024 * 1024)
                rate = mb / elapsed if elapsed > 0 else 0.0
                tail = base if len(base) <= 70 else base[:67] + "…"
                log.info(
                    "Job %s: DOWNLOAD %d/%d (~%.0f%%) ~%.2f MiB/s — %s",
                    job_id[:14],
                    done,
                    tot,
                    pct,
                    rate,
                    tail,
                )

        log.info(
            "Job %s: DOWNLOAD start (%d workers, %d files, max %d concurrent to Google)…",
            job_id[:14],
            workers,
            total,
            _DL_GATE,
        )
        results = download_parallel(tasks, workers, on_done, track=track_bytes)
        t_dl_end = time.time()
        log.info(
            "Job %s: DOWNLOAD done — %.2fs, %.1f MiB total",
            job_id[:14],
            t_dl_end - t_list_end,
            bytes_so_far() / (1024 * 1024),
        )
        tmp_paths_to_clean = [p for _i, p, _b in results]
        disk_items = [(p, b) for _i, p, b in results]

        j_update(phase="packing", pack_current=0, label="", bytes_downloaded=bytes_so_far())

        suffix = ".zip" if fmt == "zip" else ".tar.gz"
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        out_path = tmp.name
        tmp.close()

        log.info("Job %s: PACK start (%s, %d members)…", job_id[:14], fmt, total)

        def on_pack(cur: int, tot: int) -> None:
            j_update(pack_current=cur)
            if cur in (1, tot) or cur % max(1, tot // 12) == 0:
                log.info("Job %s: PACK %d/%d", job_id[:14], cur, tot)

        if fmt == "zip":
            build_zip_from_disk(disk_items, out_path, on_pack)
        else:
            build_tar_gz_from_disk(disk_items, out_path, on_pack)
        t_pack_end = time.time()

        for p in tmp_paths_to_clean:
            try:
                os.unlink(p)
            except OSError:
                pass
        tmp_paths_to_clean.clear()

        sz = os.path.getsize(out_path)
        list_s = t_list_end - t_job
        dl_s = t_dl_end - t_list_end
        pack_s = t_pack_end - t_dl_end
        total_s = t_pack_end - t_job
        mbps = (bytes_downloaded / (1024 * 1024)) / dl_s if dl_s > 0 else 0.0
        log.info(
            "Job %s: DONE — archive %s (%.1f MiB) | list=%.2fs download=%.2fs pack=%.2fs "
            "total=%.2fs | download avg ~%.2f MiB/s | workers=%d cap=%d",
            job_id[:14],
            suffix,
            sz / (1024 * 1024),
            list_s,
            dl_s,
            pack_s,
            total_s,
            mbps,
            workers,
            parallel_cap(),
        )

        download_name = f"drive-folder-{folder_id[:8]}{suffix}"
        mime = "application/zip" if fmt == "zip" else "application/gzip"
        j_update(
            phase="done",
            result_path=out_path,
            download_name=download_name,
            mime=mime,
            label="",
            pct=100.0,
        )
    except Exception as e:
        log.exception("Job %s: FAILED — %s", job_id[:14], e)
        for p in tmp_paths_to_clean:
            try:
                os.unlink(p)
            except OSError:
                pass
        j_update(phase="error", error=str(e))


@app.after_request
def cors(resp: Response):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["X-Robots-Tag"] = "index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1"
    return resp


@app.route("/")
def index():
    root = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(root, "index.html")
    return send_file(path, mimetype="text/html")


@app.route("/<path:page>")
def static_pages(page: str):
    allowed = {
        "blog-admin.html",
        "blogs.html",
        "about.html",
        "privacy-policy.html",
        "terms-and-conditions.html",
        "ads-disclosure.html",
        "contact.html",
        "favicon.png",
        "favicon.ico",
        "robots.txt",
        "sitemap.xml",
        "ads.txt",
    }
    is_dynamic_blog = bool(re.fullmatch(r"blog-[a-z0-9-]+\.html", page))
    if page not in allowed and not is_dynamic_blog:
        return jsonify({"error": "Not found"}), 404
    root = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(root, page)
    if not os.path.exists(path):
        return jsonify({"error": "Not found"}), 404
    if page == "blog-admin.html":
        auth_err = _require_admin_basic_auth()
        if auth_err is not None:
            return auth_err
    if page.endswith(".xml"):
        mime = "application/xml"
    elif page.endswith(".png") or page.endswith(".ico"):
        mime = "image/png"
    elif page.endswith(".txt"):
        mime = "text/plain; charset=utf-8"
    else:
        mime = "text/html"
    return send_file(path, mimetype=mime)


def _require_admin_basic_auth():
    user_expected = os.environ.get("BLOG_ADMIN_USER", "driveimgtojpg")
    pass_expected = os.environ.get("BLOG_ADMIN_PASS", "Adm!n@1@$$32#")
    auth = request.authorization
    ok = bool(
        auth
        and hmac.compare_digest(auth.username or "", user_expected)
        and hmac.compare_digest(auth.password or "", pass_expected)
    )
    if ok:
        return None
    resp = Response("Authentication required.", 401)
    resp.headers["WWW-Authenticate"] = 'Basic realm="GD Blog Admin"'
    return resp


def _blog_slug(raw: str) -> str:
    s = (raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return re.sub(r"-{2,}", "-", s)[:80] or f"post-{int(time.time())}"


def _blog_index_path() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, "blog-index.json")


def _blog_media_dir() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    p = os.path.join(root, "blog-media")
    os.makedirs(p, exist_ok=True)
    return p


def _load_blog_index() -> list[dict]:
    p = _blog_index_path()
    if not os.path.exists(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception:
        return []
    return []


def _save_blog_index(items: list[dict]) -> None:
    with open(_blog_index_path(), "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=True, indent=2)


def _build_blog_html(
    *,
    title: str,
    description: str,
    keywords: str,
    category: str,
    tags: str,
    author_name: str,
    published_at: int,
    featured_image: str,
    links_html: str,
    slug: str,
    content_html: str,
) -> str:
    canonical = f"https://drive.imgtojpg.org/blog-{slug}.html"
    title_e = html.escape(title)
    desc_e = html.escape(description)
    kw_e = html.escape(keywords)
    cat_e = html.escape(category)
    tags_e = html.escape(tags)
    author_e = html.escape(author_name or "Elephic Team")
    dt_str = datetime.utcfromtimestamp(max(1, int(published_at))).strftime("%B %d, %Y")
    feat_e = html.escape(featured_image)
    links_block = ""
    if links_html.strip():
        links_block = f'<h2>Useful Links</h2>\\n      <div>{links_html}</div>'
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="description" content="{desc_e}" />
  <meta name="keywords" content="{kw_e}" />
  <meta name="robots" content="index, follow" />
  <meta name="author" content="Elephic Technologies" />
  <meta name="publisher" content="Elephic Technologies" />
  <meta property="og:type" content="article" />
  <meta property="og:title" content="{title_e}" />
  <meta property="og:description" content="{desc_e}" />
  <meta property="og:url" content="{canonical}" />
  <meta property="og:image" content="{feat_e}" />
  <link rel="canonical" href="{canonical}" />
  <link rel="shortcut icon" href="/favicon.ico" />
  <link rel="icon" type="image/png" href="/favicon.png" />
  <link rel="apple-touch-icon" href="/favicon.png" />
  <title>{title_e} | GD Downloader Blog</title>
  <style>
    body {{ margin: 0; font-family: Inter, system-ui, -apple-system, sans-serif; background: #f3f4f6; color: #111827; }}
    .page {{ max-width: 52rem; margin: 0 auto; padding: 2rem 1.25rem 3rem; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 1rem; padding: 1.5rem; }}
    h1, h2, h3, h4 {{ line-height: 1.3; color: #111827; }}
    p, li {{ line-height: 1.78; color: #374151; }}
    a {{ color: #2563eb; text-decoration: none; }}
    .meta {{ color: #6b7280; font-size: .9rem; margin-bottom: 1rem; }}
    .nav {{ margin-top: 1.3rem; display: flex; gap: .9rem; flex-wrap: wrap; font-size: .95rem; }}
    .cover {{ width: 100%; border-radius: .8rem; margin: .75rem 0 1rem; }}
  </style>
</head>
<body>
  <main class="page">
    <article class="card">
      <h1>{title_e}</h1>
      <p class="meta">Via {author_e} · Published {dt_str} · Category: {cat_e} · Tags: {tags_e}</p>
      {"<img class='cover' src='" + feat_e + "' alt='" + title_e + " | drive.imgtojpg.org' />" if feat_e else ""}
      {content_html}
      {links_block}
      <p class="nav"><a href="/blogs.html">All Blogs</a> <a href="/">Home</a> <a href="/contact.html">Contact</a></p>
    </article>
  </main>
</body>
</html>
"""


def _upsert_blog_in_hub(title: str, slug: str, description: str) -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    hub = os.path.join(root, "blogs.html")
    if not os.path.exists(hub):
        return
    with open(hub, "r", encoding="utf-8") as f:
        txt = f.read()
    item = (
        f'        <div class="item"><a href="/blog-{slug}.html">{html.escape(title)}</a>'
        f"<p>{html.escape(description)}</p></div>\n"
    )
    top_marker = "      <div class=\"list\">\n"
    txt = re.sub(
        rf'\n?\s*<div class="item"><a href="/blog-{re.escape(slug)}\.html">.*?</div>\n',
        "\n",
        txt,
        flags=re.S,
    )
    if top_marker in txt:
        txt = txt.replace(top_marker, top_marker + item, 1)
        with open(hub, "w", encoding="utf-8") as f:
            f.write(txt)


def _upsert_blog_in_sitemap(slug: str) -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    sm = os.path.join(root, "sitemap.xml")
    if not os.path.exists(sm):
        return
    with open(sm, "r", encoding="utf-8") as f:
        txt = f.read()
    loc = f"https://drive.imgtojpg.org/blog-{slug}.html"
    txt = re.sub(
        rf'\s*<url><loc>{re.escape(loc)}</loc><changefreq>monthly</changefreq><priority>0.7</priority></url>\n?',
        "",
        txt,
    )
    row = (
        f"  <url><loc>{loc}</loc><changefreq>monthly</changefreq>"
        f"<priority>0.7</priority></url>\n"
    )
    txt = txt.replace("</urlset>", row + "</urlset>")
    with open(sm, "w", encoding="utf-8") as f:
        f.write(txt)


def _delete_blog_from_hub_and_sitemap(slug: str) -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    hub = os.path.join(root, "blogs.html")
    if os.path.exists(hub):
        with open(hub, "r", encoding="utf-8") as f:
            txt = f.read()
        txt = re.sub(
            rf'\n?\s*<div class="item"><a href="/blog-{re.escape(slug)}\.html">.*?</div>\n',
            "\n",
            txt,
            flags=re.S,
        )
        with open(hub, "w", encoding="utf-8") as f:
            f.write(txt)
    sm = os.path.join(root, "sitemap.xml")
    if os.path.exists(sm):
        with open(sm, "r", encoding="utf-8") as f:
            txt = f.read()
        loc = f"https://drive.imgtojpg.org/blog-{slug}.html"
        txt = re.sub(
            rf'\s*<url><loc>{re.escape(loc)}</loc><changefreq>monthly</changefreq><priority>0.7</priority></url>\n?',
            "",
            txt,
        )
        with open(sm, "w", encoding="utf-8") as f:
            f.write(txt)


@app.route("/blog-media/<path:name>", methods=["GET"])
def blog_media(name: str):
    d = _blog_media_dir()
    safe_name = os.path.basename(name)
    p = os.path.join(d, safe_name)
    if not os.path.exists(p):
        return jsonify({"error": "Not found"}), 404
    ext = os.path.splitext(safe_name)[1].lower()
    mime = "application/octet-stream"
    if ext in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif ext == ".png":
        mime = "image/png"
    elif ext == ".webp":
        mime = "image/webp"
    elif ext == ".gif":
        mime = "image/gif"
    return send_file(p, mimetype=mime)


@app.route("/api/admin/blog/upload-image", methods=["POST", "OPTIONS"])
def admin_blog_upload_image():
    if request.method == "OPTIONS":
        return ("", 204)
    auth_err = _require_admin_basic_auth()
    if auth_err is not None:
        return auth_err
    f = request.files.get("image")
    if f is None or not f.filename:
        return jsonify({"error": "No file uploaded"}), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        return jsonify({"error": "Unsupported image type"}), 400
    name = f"{int(time.time())}-{uuid.uuid4().hex[:8]}{ext}"
    out = os.path.join(_blog_media_dir(), name)
    f.save(out)
    return jsonify({"ok": True, "url": f"/blog-media/{name}"})


@app.route("/api/admin/blog/list", methods=["GET"])
def admin_blog_list():
    auth_err = _require_admin_basic_auth()
    if auth_err is not None:
        return auth_err
    items = sorted(_load_blog_index(), key=lambda x: x.get("updated_at", 0), reverse=True)
    return jsonify({"ok": True, "items": items})


@app.route("/api/admin/blog/<slug>", methods=["GET", "PUT", "DELETE", "OPTIONS"])
def admin_blog_item(slug: str):
    if request.method == "OPTIONS":
        return ("", 204)
    auth_err = _require_admin_basic_auth()
    if auth_err is not None:
        return auth_err
    items = _load_blog_index()
    i = next((idx for idx, it in enumerate(items) if it.get("slug") == slug), None)
    if i is None:
        return jsonify({"error": "Blog not found"}), 404
    item = items[i]

    if request.method == "GET":
        return jsonify({"ok": True, "item": item})

    if request.method == "DELETE":
        root = os.path.dirname(os.path.abspath(__file__))
        p = os.path.join(root, f"blog-{slug}.html")
        try:
            if os.path.exists(p):
                os.unlink(p)
        except OSError:
            pass
        _delete_blog_from_hub_and_sitemap(slug)
        items.pop(i)
        _save_blog_index(items)
        return jsonify({"ok": True})

    body = request.get_json(silent=True) or {}
    title = str(body.get("title") or "").strip()
    description = str(body.get("description") or "").strip()
    content_html = str(body.get("content_html") or "").strip()
    if not title or not description or not content_html:
        return jsonify({"error": "Missing title/description/content_html"}), 400
    item.update(
        {
            "title": title,
            "description": description,
            "keywords": str(body.get("keywords") or "").strip(),
            "category": str(body.get("category") or "Uncategorized").strip(),
            "tags": str(body.get("tags") or "").strip(),
            "author_name": str(body.get("author_name") or item.get("author_name") or "Elephic Team").strip(),
            "featured_image": str(body.get("featured_image") or "").strip(),
            "links_html": str(body.get("links_html") or "").strip(),
            "content_html": content_html,
            "updated_at": int(time.time()),
        }
    )
    html_doc = _build_blog_html(
        title=item["title"],
        description=item["description"],
        keywords=item["keywords"],
        category=item["category"],
        tags=item["tags"],
        author_name=item["author_name"],
        published_at=int(item.get("created_at") or int(time.time())),
        featured_image=item["featured_image"],
        links_html=item["links_html"],
        slug=slug,
        content_html=item["content_html"],
    )
    root = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(root, f"blog-{slug}.html"), "w", encoding="utf-8") as f:
        f.write(html_doc)
    _upsert_blog_in_hub(item["title"], slug, item["description"])
    _upsert_blog_in_sitemap(slug)
    _save_blog_index(items)
    return jsonify({"ok": True, "url": f"/blog-{slug}.html"})


@app.route("/api/admin/blog/create", methods=["POST", "OPTIONS"])
def admin_blog_create():
    if request.method == "OPTIONS":
        return ("", 204)
    auth_err = _require_admin_basic_auth()
    if auth_err is not None:
        return auth_err
    body = request.get_json(silent=True) or {}
    required = ["title", "description", "content_html"]
    for k in required:
        if not str(body.get(k) or "").strip():
            return jsonify({"error": f"Missing {k}"}), 400

    token_required = os.environ.get("BLOG_ADMIN_TOKEN", "").strip()
    if token_required:
        token = str(body.get("token") or "").strip()
        if token != token_required:
            return jsonify({"error": "Invalid admin token"}), 403

    title = str(body.get("title") or "").strip()
    description = str(body.get("description") or "").strip()
    keywords = str(body.get("keywords") or "").strip()
    category = str(body.get("category") or "Uncategorized").strip()
    tags = str(body.get("tags") or "").strip()
    author_name = str(body.get("author_name") or "Elephic Team").strip()
    featured = str(body.get("featured_image") or "").strip()
    links_html = str(body.get("links_html") or "").strip()
    slug = _blog_slug(str(body.get("slug") or title))
    content_html = str(body.get("content_html") or "").strip()
    filename = f"blog-{slug}.html"

    root = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(root, filename)
    if os.path.exists(path):
        return jsonify({"error": f"Blog already exists: {filename}"}), 409

    html_doc = _build_blog_html(
        title=title,
        description=description,
        keywords=keywords,
        category=category,
        tags=tags,
        author_name=author_name,
        published_at=int(time.time()),
        featured_image=featured,
        links_html=links_html,
        slug=slug,
        content_html=content_html,
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    now = int(time.time())
    items = _load_blog_index()
    items.insert(
        0,
        {
            "slug": slug,
            "title": title,
            "description": description,
            "keywords": keywords,
            "category": category,
            "tags": tags,
            "author_name": author_name,
            "featured_image": featured,
            "links_html": links_html,
            "content_html": content_html,
            "created_at": now,
            "updated_at": now,
        },
    )
    _save_blog_index(items)
    _upsert_blog_in_hub(title, slug, description)
    _upsert_blog_in_sitemap(slug)
    return jsonify({"ok": True, "slug": slug, "url": f"/blog-{slug}.html"})


@app.route("/api/health", methods=["GET"])
def health():
    k = api_key()
    return jsonify(
        {
            "ok": True,
            "api_key_configured": bool(k),
            "folder_max_depth": FOLDER_MAX_DEPTH,
            "max_files_cap": MAX_FILES_CAP,
            "parallel_workers": parallel_cap(),
            "max_simultaneous_downloads": _DL_GATE,
        }
    )


@app.route("/api/folder-archive/start", methods=["POST", "OPTIONS"])
def folder_archive_start():
    if request.method == "OPTIONS":
        return ("", 204)
    sweep_stale_jobs()
    key = api_key()
    if not key:
        return (
            jsonify(
                {
                    "error": "Missing GOOGLE_API_KEY (or GDRIVE_API_KEY). "
                    "Enable the Google Drive API on your Google Cloud project and set the key.",
                }
            ),
            503,
        )
    body = request.get_json(silent=True) or {}
    raw = (body.get("url_or_id") or "").strip()
    fmt = (body.get("format") or "zip").lower()
    if fmt not in ("zip", "tar.gz", "tgz"):
        return jsonify({"error": 'format must be "zip" or "tar.gz"'}), 400
    if fmt == "tgz":
        fmt = "tar.gz"
    folder_id = extract_folder_id(raw)
    if not folder_id:
        return jsonify({"error": "Could not parse a folder ID from the input."}), 400

    job_id = secrets.token_urlsafe(18)
    with jobs_lock:
        jobs[job_id] = {
            "phase": "queued",
            "total": 0,
            "total_bytes": 0,
            "sized_files": 0,
            "bytes_downloaded": 0,
            "completed": 0,
            "pack_current": 0,
            "label": "",
            "error": None,
            "result_path": None,
            "download_name": None,
            "mime": None,
            "created": time.time(),
            "workers": parallel_workers(),
        }

    t = threading.Thread(
        target=run_archive_job,
        args=(job_id, key, raw, FOLDER_MAX_DEPTH, fmt, folder_id),
        daemon=True,
    )
    t.start()
    log.info(
        "Job %s… started (folder=%s…, format=%s, parallel_cap=%s)",
        job_id[:10],
        folder_id[:12],
        fmt,
        parallel_cap(),
    )
    return jsonify({"job_id": job_id})


@app.route("/api/folder-archive/status/<job_id>", methods=["GET"])
def folder_archive_status(job_id: str):
    sweep_stale_jobs()
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Unknown or expired job.", "phase": "error"}), 404
    phase = job.get("phase", "")
    err = job.get("error")
    if phase == "error" and err:
        return jsonify(
            {
                "phase": "error",
                "error": err,
                "pct": 0,
                "detail": err,
                "total": job.get("total", 0),
                "completed": job.get("completed", 0),
                "total_bytes": job.get("total_bytes", 0),
                "sized_files": job.get("sized_files", 0),
                "bytes_downloaded": job.get("bytes_downloaded", 0),
            }
        )
    pct = job_pct(job)
    return jsonify(
        {
            "phase": phase,
            "total": job.get("total", 0),
            "completed": job.get("completed", 0),
            "pack_current": job.get("pack_current", 0),
            "total_bytes": job.get("total_bytes", 0),
            "sized_files": job.get("sized_files", 0),
            "bytes_downloaded": job.get("bytes_downloaded", 0),
            "workers": job.get("workers", parallel_workers()),
            "pct": round(pct, 1),
            "detail": job_detail(job),
        }
    )


@app.route("/api/folder-archive/download/<job_id>", methods=["GET"])
def folder_archive_download(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job.get("phase") != "done":
        return jsonify({"error": "Archive not ready."}), 404
    path = job["result_path"]
    name = job["download_name"]
    mime = job["mime"]
    count = str(job.get("total", 0))

    def gen() -> Iterator[bytes]:
        try:
            with open(path, "rb") as f:
                while True:
                    b = f.read(1024 * 1024)
                    if not b:
                        break
                    yield b
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
            with jobs_lock:
                jobs.pop(job_id, None)

    return Response(
        gen(),
        mimetype=mime or "application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{name}"',
            "X-Files-Count": count,
        },
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    log.info(
        "Open http://127.0.0.1:%s — parallel_cap=%s max_concurrent_downloads=%s "
        "http_timeout=(connect=%ss read=%ss) (GDRIVE_PARALLEL / GDRIVE_MAX_SIMULTANEOUS_DOWNLOADS / timeouts).",
        port,
        parallel_workers(),
        _DL_GATE,
        HTTP_TIMEOUT[0],
        HTTP_TIMEOUT[1],
    )
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
