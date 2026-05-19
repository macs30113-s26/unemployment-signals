# AWS Pipeline (Failed Attempt — Archived)

**Status:** This directory archives the original AWS Academy Learner Lab
pipeline that was attempted for this project. **The pipeline never
completed a successful end-to-end run.** The final working pipeline
(Midway3 + Slurm + multiprocessing + Wikimedia REST API) lives in the
project root. See the project-root `README.md` §7.1 for the full
incident narrative.

## What this directory contains

- `ingest/lambda_function.py` — per-hourly-file Lambda handler. Functional
  Python; was deployed via `build_lambda_zip.sh` and invoked through
  `orchestrator.py`.
- `ingest/orchestrator.py` — local driver that fanned out Lambda invocations
  in rounds, with idempotent S3-listing between rounds for resumability.
- `ingest/verify_articles.py` — Wikipedia API smoke check used pre-deployment.
- `ingest/build_lambda_zip.sh` — Lambda deployment-package build script.
- `aggregate/aggregate_pageviews.py` — PySpark aggregation intended for EC2.
  Never ran against real data because the ingest layer never completed.
- `aggregate/athena_ddl.sql` — Glue/Athena external table DDL.
- `analyze/analyze.py` — earlier draft of analysis script.

The code in this directory is preserved for reference and to document the
engineering pivot, but is not part of the project's final deliverables.

## Why the pipeline never completed

Two cloud-pipeline attempts triggered AWS Academy Learner Lab account
deactivations within minutes of starting the dispatch:

- **Attempt 1 (Lambda reserved concurrency = 40):** account force-deactivated
  ~12 minutes into a full-window dispatch. Cumulative spend $20.30 of $50 lab
  budget. Post-deactivation IAM state included an `s3:ListBucket` explicit-deny
  that prevented standard resume logic.
- **Attempt 2 (Lambda concurrency = 8, with a hard guardrail refusing > 10):**
  account force-deactivated at ~29 minutes with $0 spent and ~2,000 of 21,000
  target hours ingested. Course staff documentation lists the per-account
  Lambda concurrency cap as 10; async-invoked Lambda retries appear to have
  transiently exceeded this cap.

No partial outputs survived deactivation (S3 buckets were also revoked along
with the IAM credentials). The migration to Midway3 + Slurm was the direct
engineering response to these two failures.
