"""
Fan-out orchestrator for the pageview ingest pipeline.

Generates a payload for every (year, month, day, hour) tuple in the analysis
window, then issues async Lambda invocations in batches. Tracks which hours
have completed by listing the staging S3 prefix between rounds, so re-runs
skip already-done work and only retry gaps.

Why a custom orchestrator instead of Step Functions or SQS:
  - Step Functions Standard charges per state transition; at ~65k events,
    that's ~$1.60 just for transitions. Custom keeps it free.
  - SQS + Lambda event source mapping would be cleaner production-wise, but
    in Learner Lab the SQS-Lambda integration sometimes hits permission
    issues the lab user can't fix. boto3 async invoke is bulletproof.
  - This script is also a clearer demonstration of "scalable computing
    strategy" for the rubric — we explicitly control concurrency and back-
    pressure rather than handing it to a managed service.

Run from a small EC2 instance (t3.micro is fine) or your laptop with
AWS credentials configured. Expects:
  - The Lambda function 'pageview-ingest' exists, has the env vars set,
    and the lab IAM role can invoke it.
  - articles.yaml has been uploaded to s3://$BUCKET/config/articles.yaml.

Usage:
  python orchestrator.py --start 2024-01-01 --end 2026-05-01 \
      --bucket my-bucket --concurrency 8 --dispatch-batch 2000 \
      --head-fallback --function pageview-ingest
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import boto3
from botocore.config import Config

LAMBDA = None  # set in main


def hour_iter(start: datetime, end: datetime):
    """Yield every hour [start, end) as (y, m, d, h)."""
    cur = start
    while cur < end:
        yield cur.year, cur.month, cur.day, cur.hour
        cur += timedelta(hours=1)


def already_done(bucket: str, prefix: str, fallback_head: bool = False,
                 targets: list[tuple[int, int, int, int]] | None = None
                 ) -> set[tuple[int, int, int, int]]:
    """List S3 staging prefix and return the set of (y,m,d,h) already written.

    Lets re-runs be cheap: we only invoke Lambda for hours that don't already
    have an output file. Crucial for resilience — if the orchestrator dies
    halfway, we resume without paying twice.

    `fallback_head=True` uses one `HEAD_object` per target instead of a single
    `ListObjectsV2`. This is dramatically slower (one HTTP request per hour
    instead of one per 1000 keys) but works in environments where the lab
    role has an explicit-deny on s3:ListBucket — which is the case in some
    AWS Academy Learner Lab variants.
    """
    s3 = boto3.client("s3")
    done: set[tuple[int, int, int, int]] = set()

    if fallback_head:
        if not targets:
            return done
        # HEAD each target key. We use a threadpool because per-call latency
        # dominates throughput — sequential HEADs at 50ms each across 65k
        # objects would take an hour; with 32 threads it's ~2 minutes.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def head_one(t):
            y, m, d, h = t
            key = (
                f"{prefix.rstrip('/')}"
                f"/year={y:04d}/month={m:02d}/day={d:02d}/hour={h:02d}.jsonl.gz"
            )
            try:
                s3.head_object(Bucket=bucket, Key=key)
                return t
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=32) as pool:
            futures = [pool.submit(head_one, t) for t in targets]
            for i, fut in enumerate(as_completed(futures), 1):
                r = fut.result()
                if r is not None:
                    done.add(r)
                if i % 5000 == 0:
                    print(f"  HEAD-checked {i}/{len(targets)}, {len(done)} found")
        return done

    # Normal path: paginated ListObjectsV2
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix.rstrip("/") + "/"}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            # Expect key shape:
            #   <prefix>/year=YYYY/month=MM/day=DD/hour=HH.jsonl.gz
            key = obj["Key"]
            try:
                parts = key.split("/")
                y = int(next(p for p in parts if p.startswith("year=")).split("=")[1])
                m = int(next(p for p in parts if p.startswith("month=")).split("=")[1])
                d = int(next(p for p in parts if p.startswith("day=")).split("=")[1])
                # Last segment is "hour=HH.jsonl.gz" or legacy "hour=HH.parquet"
                last = parts[-1]
                if not last.startswith("hour="):
                    continue
                hh = last.split("=")[1].split(".")[0]
                h = int(hh)
                done.add((y, m, d, h))
            except (StopIteration, ValueError, IndexError):
                continue
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return done


def invoke_one(function_name: str, payload: dict) -> tuple[dict, bool, str]:
    """Synchronously-from-the-client-side invoke a single Lambda.

    We use InvocationType='Event' (async fire-and-forget) for throughput.
    Errors then surface only via CloudWatch — but because we re-check S3 at
    the end and re-invoke gaps, missing outputs get healed automatically.
    """
    try:
        LAMBDA.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(payload).encode(),
        )
        return payload, True, ""
    except Exception as e:
        return payload, False, f"{type(e).__name__}: {e}"


def run_round(function_name: str, payloads: list[dict], concurrency: int) -> int:
    """Fire one round of async invocations with bounded concurrency."""
    failures = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(invoke_one, function_name, p) for p in payloads]
        for i, fut in enumerate(as_completed(futures), 1):
            payload, ok, err = fut.result()
            if not ok:
                failures += 1
                if failures <= 5:  # don't spam
                    print(f"  invoke failed for {payload}: {err}", file=sys.stderr)
            if i % 500 == 0:
                print(f"  dispatched {i}/{len(payloads)}")
    return failures


def main():
    global LAMBDA
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD exclusive")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--staging-prefix", default="staging")
    ap.add_argument("--function", default="pageview-ingest")
    ap.add_argument("--concurrency", type=int, default=8,
                    help="Client-side dispatch concurrency. AWS Academy "
                         "Learner Lab caps concurrent Lambda executions at "
                         "10 — exceeding this triggers lab deactivation. "
                         "Default 8 leaves headroom for retries. Hard "
                         "ceiling of 10 enforced.")
    ap.add_argument("--max-rounds", type=int, default=50,
                    help="How many passes to make filling in S3 gaps. With "
                         "default --dispatch-batch 2000 and a 7-year window "
                         "(~65k hours), ~33 rounds cover the full set. "
                         "Set higher for resilience to failed rounds.")
    ap.add_argument("--head-fallback", action="store_true",
                    help="Use s3:HeadObject per target instead of "
                         "s3:ListObjectsV2. Slower but works under the "
                         "explicit-deny on ListBucket that some AWS "
                         "Academy Learner Lab variants apply.")
    ap.add_argument("--dispatch-batch", type=int, default=2000,
                    help="Hours per round. At concurrency 8 and ~3s/file, "
                         "2000 invocations take ~12 minutes per round. "
                         "Smaller batches = safer for the lab's abuse "
                         "detector but slower wall-clock.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Hard guardrail: AWS Academy Learner Lab limits concurrent Lambda
    # executions to 10. Exceeding this triggered account deactivation on
    # 2026-05-18 (see README §7). We refuse to run with concurrency > 10
    # to prevent retriggering. Lower if you have an error budget concern;
    # the TA recommends 3 for initial debugging.
    LEARNER_LAB_CONCURRENCY_CAP = 10
    if args.concurrency > LEARNER_LAB_CONCURRENCY_CAP:
        sys.exit(
            f"\nERROR: --concurrency={args.concurrency} exceeds the AWS Academy "
            f"Learner Lab cap of {LEARNER_LAB_CONCURRENCY_CAP} concurrent "
            f"Lambda executions. Exceeding this triggers lab deactivation.\n"
            f"Use --concurrency {LEARNER_LAB_CONCURRENCY_CAP} or lower "
            f"(the TA suggests 3 for debugging, 8 for steady-state runs).\n"
        )

    start = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d")

    # Build the full target set.
    all_targets = list(hour_iter(start, end))
    print(f"Full window: {len(all_targets)} hours from {start} to {end}")

    LAMBDA = boto3.client(
        "lambda",
        # Async invoke API can throttle at high QPS — generous retries help.
        config=Config(retries={"max_attempts": 10, "mode": "adaptive"}),
    )

    for round_idx in range(1, args.max_rounds + 1):
        done = already_done(
            args.bucket,
            args.staging_prefix,
            fallback_head=args.head_fallback,
            targets=all_targets if args.head_fallback else None,
        )
        pending = [(y, m, d, h) for (y, m, d, h) in all_targets if (y, m, d, h) not in done]
        print(f"\nRound {round_idx}: {len(done)} done, {len(pending)} pending")

        if not pending:
            print("All hours have output in S3. Done.")
            break

        # Take only `dispatch_batch` items per round. This caps the simultaneous
        # in-flight Lambda count and is the change that fixes the lab-deactivation
        # incident we hit on 2026-05-17: bursting 65k async invocations in one
        # shot triggered AWS Academy's abuse heuristic. Smaller, slower rounds
        # are safer in this environment.
        batch = pending[: args.dispatch_batch]
        payloads = [{"year": y, "month": m, "day": d, "hour": h} for (y, m, d, h) in batch]

        if args.dry_run:
            print("--dry-run set; would invoke", len(payloads), "Lambdas. First 3:")
            for p in payloads[:3]:
                print(" ", p)
            return

        t0 = time.time()
        failures = run_round(args.function, payloads, args.concurrency)
        elapsed = time.time() - t0
        rate = len(payloads) / elapsed if elapsed > 0 else 0
        print(f"  dispatched {len(payloads)} invocations in {elapsed:.0f}s "
              f"({rate:.0f}/sec), {failures} client-side errors")

        if round_idx < args.max_rounds:
            # Sleep proportional to batch size: 5000 invocations at concurrency 20
            # = ~250 batches of 1 in flight × ~3s each = ~12 min in-flight.
            # We sleep 5 minutes to give plenty of time without burning the
            # 4-hour Learner Lab session window.
            wait_s = 300
            print(f"  sleeping {wait_s}s to let invocations complete "
                  f"({len(pending) - len(batch)} hours still pending)...")
            time.sleep(wait_s)


if __name__ == "__main__":
    main()
