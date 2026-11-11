"""Step 2: Transform — รวม schema ของ 4 taxi types เป็น schema กลาง แล้วเขียน curated Parquet

Curated schema (data/curated/trips_unified/taxi=<slug>/month=<YYYY-MM>/data.parquet):
  taxi_type, source_file, file_month, pickup_datetime, dropoff_datetime,
  pu_location_id, do_location_id, invalid_reason, is_valid, dropoff_before_pickup

หลักการ: "ไม่ลบแถว แต่ติดธงไว้" (invalid_reason) เพื่อให้ตรวจย้อนกลับได้ว่าตัดอะไรเพราะอะไร
รันซ้ำได้: ไฟล์ที่ sha256 + window เหมือนเดิมและมี partition อยู่แล้ว จะถูกข้าม
"""
import json
import logging
import os

import duckdb

from .ingest import USABLE_STATUSES, read_manifest
from .utils import now_iso, slug, sql_path, window

log = logging.getLogger("pipeline")


def state_path(cfg):
    return cfg["paths"]["curated"] / "_state.json"


def load_state(cfg) -> dict:
    p = state_path(cfg)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {"files": {}}


def save_state(cfg, state):
    cfg["paths"]["curated"].mkdir(parents=True, exist_ok=True)
    state_path(cfg).write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def curated_glob(cfg) -> str:
    return sql_path(cfg["paths"]["curated"] / "trips_unified" / "*" / "*" / "data.parquet")


def _col_expr(cols, wanted, as_int=False):
    real = cols.get(wanted.lower()) if wanted else None
    if real is None:
        return "NULL", None
    if as_int:
        return f'TRY_CAST("{real}" AS INTEGER)', real
    return f'TRY_CAST("{real}" AS TIMESTAMP)', real


def run(cfg, run_id) -> dict:
    start, end = window(cfg)
    win_key = f"{start}..{end}"
    state = load_state(cfg)
    con = duckdb.connect()
    processed, skipped, failed_schema, rows_out = 0, 0, 0, 0

    for row in read_manifest(cfg):
        if row["status"] not in USABLE_STATUSES:
            continue
        fn, taxi_type = row["filename"], row["taxi_type"]
        spec = cfg["taxi_types"][taxi_type]
        out_dir = cfg["paths"]["curated"] / "trips_unified" / f"taxi={slug(spec)}" / f"month={row['month']}"
        out_file = out_dir / "data.parquet"
        prev = state["files"].get(fn)

        if (prev and prev.get("sha256") == row["sha256"] and prev.get("window") == win_key
                and out_file.exists()):
            skipped += 1
            continue

        src = sql_path(cfg["paths"]["raw"] / fn)
        cols = {r[0].lower(): r[0] for r in
                con.execute(f"DESCRIBE SELECT * FROM read_parquet('{src}')").fetchall()}
        pick_expr, pick_real = _col_expr(cols, spec["pickup"])
        drop_expr, drop_real = _col_expr(cols, spec.get("dropoff"))
        pu_expr, pu_real = _col_expr(cols, spec.get("pu_location"), as_int=True)
        do_expr, do_real = _col_expr(cols, spec.get("do_location"), as_int=True)
        raw_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{src}')").fetchone()[0]

        entry = {"sha256": row["sha256"], "window": win_key, "raw_rows": raw_rows,
                 "resolved": {"pickup": pick_real, "dropoff": drop_real,
                              "pu": pu_real, "do": do_real},
                 "transformed_at": now_iso(), "curated_rows": 0, "schema_ok": pick_real is not None}

        if pick_real is None:
            log.error("%s: pickup column '%s' not found", fn, spec["pickup"])
            if out_file.exists():
                out_file.unlink()
            state["files"][fn] = entry
            failed_schema += 1
            save_state(cfg, state)
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        tmp = out_dir / "data.parquet.tmp"
        con.execute(f"""
            COPY (
              SELECT *,
                     (invalid_reason IS NULL) AS is_valid,
                     (pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL
                      AND dropoff_datetime < pickup_datetime) AS dropoff_before_pickup
              FROM (
                SELECT '{taxi_type}' AS taxi_type,
                       '{fn}' AS source_file,
                       '{row['month']}' AS file_month,
                       {pick_expr} AS pickup_datetime,
                       {drop_expr} AS dropoff_datetime,
                       {pu_expr} AS pu_location_id,
                       {do_expr} AS do_location_id,
                       CASE
                         WHEN {pick_expr} IS NULL THEN 'null_pickup'
                         WHEN {pick_expr} < TIMESTAMP '{start}'
                           OR {pick_expr} >= TIMESTAMP '{end}' THEN 'out_of_range'
                       END AS invalid_reason
                FROM read_parquet('{src}')
              )
            ) TO '{sql_path(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        os.replace(tmp, out_file)  # overwrite เฉพาะ partition นี้ -> idempotent
        entry["curated_rows"] = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{sql_path(out_file)}')").fetchone()[0]
        state["files"][fn] = entry
        save_state(cfg, state)
        processed += 1
        rows_out += entry["curated_rows"]
        log.info("Transformed %s: %s raw -> %s curated", fn, f"{raw_rows:,}", f"{entry['curated_rows']:,}")

    log.info("Transform: processed=%d skipped=%d schema_failed=%d", processed, skipped, failed_schema)
    return {"processed": processed, "skipped": skipped,
            "schema_failed": failed_schema, "rows_written": rows_out, "window": win_key}
