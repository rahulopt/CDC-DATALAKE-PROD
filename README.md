# Production-Grade CDC Data Lake Pipeline

A reliable, scalable, fault-tolerant Change-Data-Capture (CDC) pipeline that
streams changes from an e-commerce **Aurora PostgreSQL** OLTP database into an
analytics **lakehouse** (Apache Iceberg on S3), queryable with Athena.

> Full requirements: [`docs/requirements.html`](docs/requirements.html).
> The local learning prototype these patterns grew from lives in the sibling
> `../cdc-datalake` project.

---

## 1. Business problem

An e-commerce platform must make OLTP changes available for analytics within
~15 minutes, **including during festival/flash-sale spikes** (5k → 100k
events/min) — without losing data, duplicating rows, or letting out-of-order
events corrupt state.

## 2. Architecture

```
Aurora PostgreSQL (OLTP)
      │  CDC (INSERT/UPDATE/DELETE)
      ▼
   AWS DMS  ── full load ──▶ S3 Bronze (fullload/)
      │  ongoing CDC
      ▼
Kinesis Data Streams  (burst buffer, retention = replay/slowdown cushion)
      ▼
Glue Streaming (bronze_ingest)  ──▶  S3 Bronze (immutable, partitioned Parquet)
      ▼
Glue Batch (silver_cdc_merge)   parse → schema-check → DQ/quarantine
      │                          → dedup(latest-wins) → stale-guard → Iceberg MERGE
      ▼
Iceberg SILVER (current state, ACID, time-travel)
      ▼
Glue Batch (gold_aggregates)  ──▶  Iceberg GOLD (analytics tables)
      ▼
   Athena

Orchestration : EventBridge (rate 15m) → Step Functions → Glue → DQ
Observability : CloudWatch metrics/alarms/dashboard + DynamoDB audit
Notification  : SNS (critical only) → email
IaC           : Terraform          CI/CD: GitHub Actions
Maintenance   : nightly Glue (compaction / expire snapshots / orphan cleanup)
```

## 3. AWS services & data flow

| Stage | Service | Role |
|-------|---------|------|
| OLTP | Aurora PostgreSQL | source of truth, emits CDC |
| CDC | AWS DMS | full load → S3, ongoing CDC → Kinesis |
| Buffer | Kinesis Data Streams | absorbs bursts, decouples ingest/processing |
| Raw lake | S3 Bronze | immutable partitioned CDC, replay source |
| Processing | Glue + PySpark | parse, validate, dedup, order, MERGE |
| Current state | Apache Iceberg (Silver) | ACID MERGE, schema evolution, time travel |
| Analytics | Iceberg (Gold) | business aggregates |
| Query | Athena | SQL over Gold/Silver |
| Orchestration | Step Functions + EventBridge | workflow, retries, branches |
| Audit | DynamoDB | per-batch execution metadata |
| Monitoring | CloudWatch | metrics, alarms, dashboard |
| Alerts | SNS | critical-only notifications |
| Secrets | Secrets Manager | Aurora credentials |
| Encryption | KMS | one CMK for S3/Kinesis/DDB/SNS/Logs/Secrets |
| IaC | Terraform | all of the above |
| CI/CD | GitHub Actions | test → lint → tf validate → build → deploy |

## 4. CDC strategy

DMS captures INSERT/UPDATE/DELETE. We normalise every DMS envelope (nested
Kinesis `metadata`/`data` **or** flat S3 `Op` column) into a **canonical** row
with control columns: `__op` (I/U/D), `__table`, `__commit_ts`, `__txn_id`,
`__seq`, `__source_pos`. See `glue/cdc_lib/cdc_parser.py`.

## 5. Kinesis strategy

Kinesis is the shock absorber (req 5, 26). Processing does **not** have to keep
pace with arrival: records buffer in Kinesis (retention `kinesis_retention_hours`)
and in Bronze. Consumer lag is alarmed (`GetRecords.IteratorAgeMilliseconds` >
15 min). Scale = add shards (`kinesis_shard_count`); write throttling is alarmed.

## 6. Bronze / Silver / Gold design

* **Bronze** — append-only raw CDC, Parquet, partitioned `table/year/month/day/hour`.
  Full CDC metadata retained → complete replayability. Lifecycle tiers to
  IA/Glacier then expires (`bronze_expire_days`).
* **Silver** — Iceberg *current state*. One row per PK, maintained by MERGE.
  Stores a `__silver_commit_ts` watermark for the stale-event guard.
* **Gold** — Iceberg analytics tables (`gold_daily_sales`,
  `gold_customer_metrics`, `gold_product_metrics`, `gold_order_status`).

## 7. Iceberg design

ACID MERGE, snapshot isolation, schema evolution (`ADD COLUMN`), and time travel.
Glue jobs use the Glue Data Catalog Iceberg catalog (`glue_catalog`) with
`S3FileIO`. Maintenance job compacts small files and expires snapshots/orphans.

## 8. Idempotency 

* An `_applied_batches` Iceberg ledger records `(batch_id, table_name)`; a
  re-run of the same batch is skipped.
* `latest_per_key` collapses duplicates within a batch.
* MERGE itself is idempotent (re-applying the same newest row is a no-op).
* Verified by `tests/test_cdc_merge_e2e.py::test_idempotent_reapply`.

## 9. Ordering

`ordering.py` ranks rows by `__commit_ts, __txn_id, __seq` (desc, nulls last)
so the newest source change wins regardless of arrival order. `guard_stale_updates`
additionally drops *late* events (later batch, older commit ts) that would
regress Silver (e.g. DELIVERED → SHIPPED). Covered by `tests/test_ordering.py`.

## 10. Schema evolution 

`schema.diff_schema` classifies changes:
* **additive** (new column, widening) → `ALTER TABLE ADD COLUMN` and continue.
* **breaking** (drop/rename/incompatible type) → stop the table, write a FAILED
  audit record, publish an SNS `Schema-Breaking Change` alert, exit non-zero.
Covered by `tests/test_schema_evolution.py`.

## 11. Data quality & quarantine 

Declarative rules in `config/pipeline.yaml` (`not_null`, `non_negative`,
`allowed_values`, `valid_timestamp`) + a structural op check. Invalid rows are
written to **S3 Reject** as `{original_event, error_reason, table_name,
processing_time, batch_id}` — never silently dropped. DELETEs are exempt from
payload rules (a delete may carry only the key). See `tests/test_validation.py`.

## 12. Failure recovery & retries 

* Step Functions retries Glue/Lambda with exponential backoff (3 attempts).
* Because processing is idempotent, retries are safe.
* A failed batch is **restartable** — no manual target fix-ups needed. Re-running
  reads Bronze again; the ledger + MERGE keep state correct.
* Any unrecovered failure → `notify` Lambda → SNS alert; FAILED audit row.

## 13. Replay 

Bronze is the immutable source of truth. Replay = re-run the Silver job over a
Bronze prefix:
* **Full** — all partitions. **Date/partition** — a `year=/month=/day=` prefix.
* **Batch / failed-batch** — a specific `batch_id`. To force reprocessing,
  delete that batch's row from `_applied_batches`.

## 14. Orchestration 

`orchestration/state_machine.asl.json`: `CheckInput` (Lambda) → `HasData`
choice → `ProcessTables` (Map, per-table Silver MERGE via `glue:startJobRun.sync`
with retries/catch) → `GoldAggregates` → success. Failures caught → `NotifyFailure`.
Triggered every 15 min by EventBridge; nightly maintenance on a separate cron.

## 15. Monitoring 

CloudWatch alarms: Kinesis lag, Kinesis write throttle, Silver Glue failure,
Step Functions failure — all → SNS. Dashboard shows Kinesis throughput/lag and
Step Functions execution health. Per-batch counts land in DynamoDB.

## 16. Notifications 

SNS, **critical-only** (failures, high lag, DQ failure, schema break, SLA). Happy
path is tracked via CloudWatch/DynamoDB to avoid alert fatigue. Message format in
`cdc_lib/notifications.py` and `lambda/notify`.

## 17. Audit 

`DynamoDB` item per batch: counts (source/processed/insert/update/delete/
duplicate/reject), status, timings, error. PK `batch_table` (`batch#table`),
GSI `by_table` for per-table history. Model in `cdc_lib/audit.py`.

## 18. Security 

IAM least-privilege per role (Glue/DMS/SFN/EventBridge/Lambda), one KMS CMK
encrypting everything, S3 Block Public Access + SSE-KMS + versioning, Secrets
Manager for DB creds, DMS via Secrets Manager, CloudWatch Logs encrypted. **No
secrets in git** — `.gitignore` blocks `*.tfvars` (except examples), `.env`,
keys, credential files.

## 19. Terraform 

`terraform/` provisions everything. Layout: `providers • variables • storage
(KMS/S3) • streaming_audit (Kinesis/DynamoDB/SNS/Secrets) • iam • glue • dms •
orchestration (Lambda/SFN/EventBridge) • monitoring • outputs`. DMS resources are
conditional (`dms_subnet_ids` + `aurora_host`) so you can stand up the lake first.

```bash
make build                       # produce dist/cdc_lib.zip (referenced by Glue)
cd terraform
terraform init
terraform plan  -var-file=example.tfvars
terraform apply -var-file=example.tfvars
```

## 20. CI/CD 

`.github/workflows/ci-cd.yml`: on push/PR → unit tests + lint + `terraform
validate`; on push to `main` → build artifacts + `terraform apply` (AWS via
OIDC, no long-lived keys). Mirrors: Git Push → Tests → Lint → TF Validate →
Build → Deploy.

## 21. Testing 

```bash
make install
make test        # 34 unit tests
```

Unit tests cover CDC parsing, dedup, ordering (incl. out-of-order/late),
transformation (real Iceberg MERGE I/U/D), validation/quarantine, schema
evolution, config, audit (moto DynamoDB), notifications (moto SNS). Mandatory
scenarios from req 25 are exercised across `tests/`.

> **Local Spark note:** Glue 4.0 runs Spark 3.3/Python 3.10. Locally we pin
> `pyspark==3.5.3` (3.3 is incompatible with Python 3.11+/Java 21). The
> DataFrame/Window/SQL-MERGE APIs used are identical, so tests validate the same
> logic. `make test` unsets a shadowing `SPARK_HOME` and prefers `openjdk@17`.

## 22. Cost considerations 
DynamoDB on-demand; Kinesis provisioned + planned shards; S3 lifecycle tiering
+ expiry; CloudWatch log retention (`log_retention_days`); nightly Iceberg
compaction to avoid small-file scan costs; Glue right-sized workers; Athena over
compacted Gold to bound scans.

## 23. Scalability 

Independent scaling: Kinesis shards, Glue workers (`glue_number_of_workers`),
Step Functions `Map` concurrency across tables, Bronze/Iceberg partitioning.
Peak needs no permanently-on max compute.

## 24. Repository layout

```
cdc-datalake-prod/
├── glue/
│   ├── cdc_lib/            # shared, unit-tested library (parser/ordering/…)
│   └── jobs/               # Glue entry-points (bronze/silver/gold/maintenance)
├── lambda/{check_input,notify}/
├── orchestration/state_machine.asl.json
├── terraform/              # all IaC
├── tests/                  # pytest suite
├── config/pipeline.yaml    # data-driven table/DQ config
├── sql/                    # Aurora DDL + Athena queries
├── scripts/generate_sample_cdc.py
├── docs/requirements.html
├── .github/workflows/ci-cd.yml
├── Makefile · requirements*.txt · pyproject.toml
```

## 25. Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| Silver SLA breach / high `IteratorAge` | consumer can't keep up | add Kinesis shards; raise Glue workers/Map concurrency |
| `Schema-Breaking Change` alert | source dropped/retyped a column | review DDL, evolve Silver, then replay the batch |
| Excessive rejects | bad source data / rule mismatch | inspect S3 Reject `error_reason`; fix source or rule |
| Batch stuck "already applied" | ledger has the batch | delete its `_applied_batches` row to force replay |
| Glue job fails on Iceberg | missing `--datalake-formats iceberg` | confirmed set in `glue.tf` |
| Local tests: Py4J/`AnsiCast`/DirectByteBuffer | wrong Spark/Java | `unset SPARK_HOME`; use Java 17; `pyspark==3.5.3` |

## 26. Design decisions & trade-offs

* **Kinesis (not direct S3) for CDC** — decouples ingest from processing so
  bursts never drop data; costs a stream but buys durability + backpressure.
* **Iceberg over Hudi/Delta** — first-class MERGE, hidden partitioning, mature
  Glue/Athena support, snapshot time-travel for audits/replay.
* **Latest-wins + stale-guard (not full history in Silver)** — Silver is
  current-state per the spec; history is reconstructable from immutable Bronze.
  (An SCD2 variant exists in the prototype if history-in-place is ever needed.)
* **Config-driven tables** — adding a table is a YAML edit, not code.
* **Gold recomputed each run** — simplest correct approach at this cardinality;
  swap to incremental MERGE on the daily grain if volumes grow.
* **Critical-only SNS** — success tracked in CloudWatch/DynamoDB to avoid fatigue.
* **DMS conditional in TF** — lets the lakehouse deploy before the source DB
  networking exists, easing incremental bring-up.
```
