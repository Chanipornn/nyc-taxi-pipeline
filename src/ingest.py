"""Step 1: Ingest — ตรวจ/ดาวน์โหลด source files และเขียน manifest

รันซ้ำได้: ไฟล์ที่ ETag/ขนาด/Last-Modified ไม่เปลี่ยน -> SKIPPED (ไม่ดาวน์โหลดซ้ำ)
สถานะใน manifest: DOWNLOADED | SKIPPED | LOCAL (offline) | UNAVAILABLE | MISSING
"""
import csv
import hashlib
import logging

import requests

from .utils import build_file_list, now_iso

log = logging.getLogger("pipeline")

MANIFEST_FIELDS = ["taxi_type", "month", "filename", "url", "http_status",
                   "size_bytes", "etag", "last_modified", "sha256", "status", "checked_at"]
USABLE_STATUSES = ("DOWNLOADED", "SKIPPED", "LOCAL")


def manifest_path(cfg):
    return cfg["paths"]["reports"] / "manifest.csv"


def read_manifest(cfg) -> list:
    p = manifest_path(cfg)
    if not p.exists():
        return []
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _download(url, dest):
    tmp = dest.with_suffix(dest.suffix + ".part")
    h = hashlib.sha256()
    size = 0
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)
    tmp.replace(dest)
    return size, h.hexdigest()


def run(cfg, run_id, offline=None) -> dict:
    offline = cfg.get("offline", False) if offline is None else offline
    raw = cfg["paths"]["raw"]
    raw.mkdir(parents=True, exist_ok=True)
    prev = {r["filename"]: r for r in read_manifest(cfg)}
    rows = []

    for f in build_file_list(cfg):
        path = raw / f["filename"]
        p = prev.get(f["filename"])
        row = {**f, "http_status": "", "size_bytes": "", "etag": "", "last_modified": "",
               "sha256": "", "status": "", "checked_at": now_iso()}

        if offline:
            if path.exists():
                size = path.stat().st_size
                reuse = p and p.get("sha256") and p.get("size_bytes") == str(size)
                row.update(size_bytes=size, status="LOCAL",
                           sha256=p["sha256"] if reuse else _sha256(path))
            else:
                row["status"] = "MISSING"
        else:
            try:
                r = requests.head(f["url"], timeout=30, allow_redirects=True)
                row["http_status"] = r.status_code
                row["etag"] = r.headers.get("ETag", "")
                row["last_modified"] = r.headers.get("Last-Modified", "")
                remote_size = r.headers.get("Content-Length", "")
            except requests.RequestException as e:
                log.warning("HEAD failed %s: %s", f["filename"], e)
                r, remote_size = None, ""

            if r is not None and r.status_code == 200:
                unchanged = (path.exists() and p and p.get("sha256")
                             and p.get("etag", "") == row["etag"]
                             and p.get("last_modified", "") == row["last_modified"]
                             and p.get("size_bytes", "") == str(remote_size))
                if unchanged:
                    row.update(size_bytes=p["size_bytes"], sha256=p["sha256"], status="SKIPPED")
                else:
                    log.info("Downloading %s", f["filename"])
                    size, sha = _download(f["url"], path)
                    row.update(size_bytes=size, sha256=sha, status="DOWNLOADED")
            elif path.exists():
                row.update(size_bytes=path.stat().st_size, status="LOCAL",
                           sha256=p["sha256"] if p and p.get("sha256") else _sha256(path))
            else:
                row["status"] = "UNAVAILABLE"
        rows.append(row)

    mp = manifest_path(cfg)
    mp.parent.mkdir(parents=True, exist_ok=True)
    with open(mp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS)
        w.writeheader()
        w.writerows(rows)

    counts = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    log.info("Ingest: %s", counts)
    return {"files": len(rows), **counts}
