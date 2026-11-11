# Pipeline / Data Flow Diagram

```mermaid
flowchart LR
  A["NYC TLC<br/>4 types x 6 months<br/>24 Parquet files"] --> B["1. Ingest<br/>HEAD + ETag check<br/>skip unchanged"]
  B --> C[("data/raw")]
  B --> M[("reports/manifest.csv")]
  C --> D["2. Transform<br/>map 4 schemas -> 1 schema<br/>flag invalid rows"]
  M --> D
  D --> E[("data/curated/trips_unified<br/>taxi=*/month=*/data.parquet")]
  D --> S[("_state.json<br/>sha256 + window")]
  E --> F["3. Validate<br/>DQ checks<br/>PASS / WARN / FAIL"]
  C --> F
  F -->|FAIL| X["STOP<br/>(fail-fast)"]
  F --> Q[("reports/dq_report.csv/.json")]
  F -->|no FAIL| G["4. Analyze<br/>count valid trips"]
  E --> G
  G --> R[("monthly_summary.csv<br/>taxi_type_ranking.csv<br/>answer.md / trips_by_type.png<br/>data/sample")]
  B -. status .-> L[("reports/run_log.jsonl")]
  D -. status .-> L
  F -. status .-> L
  G -. status .-> L
```

## Curated schema (`trips_unified`)

| column | ความหมาย |
|---|---|
| taxi_type | Yellow Taxi / Green Taxi / FHV / HVFHV |
| source_file | ไฟล์ต้นทาง (ใช้ trace กลับ) |
| file_month | เดือนของไฟล์ (YYYY-MM) |
| pickup_datetime, dropoff_datetime | เวลาที่ map จาก column ของแต่ละ type |
| pu_location_id, do_location_id | zone รับ/ส่ง |
| invalid_reason | `null_pickup` / `out_of_range` / NULL |
| is_valid | TRUE ถ้า invalid_reason เป็น NULL |
| dropoff_before_pickup | ธง warning (ไม่ตัดทิ้ง) |
