"""
Streaming variant server entrypoint.

Keeps existing routes from server.py and adds:
- POST /api/folder-archive/stream
  Streams a ZIP directly to the client without writing archive/files to disk.
"""

from __future__ import annotations

import os
import queue
import threading
import zipfile

from flask import Response, jsonify, request, stream_with_context

from server import (
    FOLDER_MAX_DEPTH,
    MAX_FILES_CAP,
    STREAM_CHUNK,
    api_key,
    app,
    assign_unique_arc_names,
    collect_files,
    extract_folder_id,
    log,
    open_download_stream,
    parallel_cap,
    retry_call,
    sanitize_arc_name,
)


class _QueueWriter:
    """File-like writer that pushes bytes into a Queue for Flask streaming."""

    def __init__(self, out_q: queue.Queue[bytes | None]) -> None:
        self._q = out_q

    def write(self, data: bytes) -> int:
        if data:
            self._q.put(bytes(data))
        return len(data)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None

    def writable(self) -> bool:
        return True


def _stream_zip(tasks: list[tuple[int, str, str]]):
    out_q: queue.Queue[bytes | None] = queue.Queue(maxsize=16)
    errors: list[str] = []

    def producer() -> None:
        writer = _QueueWriter(out_q)
        try:
            with zipfile.ZipFile(
                writer,
                mode="w",
                compression=zipfile.ZIP_STORED,
                allowZip64=True,
            ) as zf:
                for _idx, file_id, arcname in tasks:
                    clean_name = sanitize_arc_name(arcname).replace("\\", "/")
                    try:
                        resp = retry_call(lambda: open_download_stream(file_id))
                        with zf.open(clean_name, mode="w", force_zip64=True) as zentry:
                            for chunk in resp.iter_content(chunk_size=STREAM_CHUNK):
                                if chunk:
                                    zentry.write(chunk)
                        resp.close()
                    except Exception as e:
                        msg = f"{clean_name}: {e}"
                        errors.append(msg)
                        log.warning("Stream ZIP skip file due to error: %s", msg)
                if errors:
                    zf.writestr("_DOWNLOAD_ERRORS.txt", "\n".join(errors) + "\n")
        finally:
            out_q.put(None)

    t = threading.Thread(target=producer, daemon=True)
    t.start()

    while True:
        item = out_q.get()
        if item is None:
            break
        yield item


@app.route("/api/folder-archive/stream", methods=["GET", "POST", "OPTIONS"])
def folder_archive_stream():
    if request.method == "OPTIONS":
        return ("", 204)

    key = api_key()
    if not key:
        return jsonify({"error": "Missing GOOGLE_API_KEY (or GDRIVE_API_KEY)."}), 500

    if request.method == "GET":
        raw = str(request.args.get("url_or_id") or "").strip()
    else:
        body = request.get_json(silent=True) or {}
        raw = str(body.get("url_or_id") or "").strip()
    folder_id = extract_folder_id(raw)
    if not folder_id:
        return jsonify({"error": "Invalid folder link/ID"}), 400

    try:
        files = collect_files(key, folder_id, FOLDER_MAX_DEPTH)
        if not files:
            return jsonify({"error": "No downloadable files found in that folder."}), 404
        tasks = assign_unique_arc_names(files)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    archive_name = f"drive-folder-{folder_id[:10]}.zip"
    log.info(
        "Streaming ZIP start (folder=%s, files=%d, cap=%d, max_files=%d)",
        folder_id[:12] + "…",
        len(tasks),
        parallel_cap(),
        MAX_FILES_CAP,
    )

    headers = {
        "Content-Disposition": f'attachment; filename="{archive_name}"',
        "X-Accel-Buffering": "no",
        "Cache-Control": "no-store",
    }
    return Response(
        stream_with_context(_stream_zip(tasks)),
        mimetype="application/zip",
        headers=headers,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    log.info("Open http://127.0.0.1:%s (stream mode enabled at /api/folder-archive/stream).", port)
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
