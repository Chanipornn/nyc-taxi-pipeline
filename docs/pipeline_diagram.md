# Pipeline / Data Flow Diagram

## 1. Data Flow

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

## 2. Architecture

```mermaid
flowchart LR
  subgraph SRC["Source"]
    TLC["NYC TLC Trip Records<br/>Yellow / Green / FHV / HVFHV<br/>Jan-Jun 2024 (24 Parquet files)"]
    CFG["config.yaml<br/>period, URL, column mapping,<br/>DQ thresholds"]
  end

  subgraph ING["1. Ingestion (src/ingest.py)"]
    HEAD["HEAD request<br/>ETag / size / Last-Modified"]
    DL["Download<br/>only new or changed files"]
    SKIP["SKIPPED<br/>unchanged files"]
  end

  subgraph STORE["Storage (local disk)"]
    RAW[("data/raw<br/>source Parquet")]
    MAN[("reports/manifest.csv<br/>url, size, sha256, status")]
    CUR[("data/curated/trips_unified<br/>taxi=*/month=*/data.parquet")]
    STATE[("data/curated/_state.json<br/>sha256 + window per file")]
  end

  subgraph PROC["2. Transform (src/transform.py, DuckDB)"]
    MAP["Map 4 schemas to 1 schema<br/>case-insensitive column lookup"]
    FLAG["Flag invalid rows<br/>null_pickup / out_of_range"]
  end

  subgraph DQ["3. Validate (src/validate.py)"]
    CHK["193 checks<br/>completeness, schema, reconciliation,<br/>null / out-of-range / reversed time,<br/>recount from raw"]
    GATE{"Any FAIL?"}
  end

  subgraph OUT["4. Analyze (src/analyze.py)"]
    MET["Count valid trips<br/>per taxi type"]
    ANS["answer.md<br/>monthly_summary.csv<br/>taxi_type_ranking.csv<br/>trips_by_type.png<br/>data/sample"]
  end

  subgraph OBS["Observability"]
    RUNLOG[("reports/run_log.jsonl")]
    PLOG[("reports/pipeline.log")]
    DQR[("reports/dq_report.csv / .json")]
  end

  ORCH["run_pipeline.py<br/>orchestrator, run_id, exit code"]

  TLC --> HEAD
  CFG -.-> HEAD
  CFG -.-> MAP
  CFG -.-> CHK
  HEAD -->|changed or new| DL
  HEAD -->|unchanged| SKIP
  DL --> RAW
  DL --> MAN
  SKIP --> MAN
  RAW --> MAP
  MAN --> MAP
  MAP --> FLAG
  FLAG --> CUR
  FLAG --> STATE
  CUR --> CHK
  RAW --> CHK
  CHK --> DQR
  CHK --> GATE
  GATE -->|yes| STOP["STOP<br/>exit code 2"]
  GATE -->|no| MET
  CUR --> MET
  MET --> ANS

  ORCH -.controls.-> ING
  ORCH -.controls.-> PROC
  ORCH -.controls.-> DQ
  ORCH -.controls.-> OUT
  ORCH --> RUNLOG
  ORCH --> PLOG
```

## 3. Curated schema (`trips_unified`)

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