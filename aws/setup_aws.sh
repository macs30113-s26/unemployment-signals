#!/usr/bin/env bash
# AWS resource setup for the pageview ingest pipeline.
#
# Run this AFTER you have:
#   1. Started the AWS Academy Learner Lab (so AWS_ACCESS_KEY_ID etc. are valid)
#   2. Copied the lab credentials into ~/.aws/credentials
#
# This script:
#   - Creates the S3 bucket (idempotent)
#   - Uploads articles.yaml to s3://$BUCKET/config/
#   - Builds the Lambda zip
#   - Creates the Lambda function 'pageview-ingest' (or updates code if it exists)
#
# It does NOT create the IAM role — Learner Lab restricts IAM creation. Instead,
# the Lambda is attached to the pre-existing 'LabRole' that every Learner Lab
# account has, which grants the same broad permissions the lab user has.
#
# Usage:
#   BUCKET=lsc4ss-wiki-attention-yourname bash setup_aws.sh

set -euo pipefail

if [[ -z "${BUCKET:-}" ]]; then
    echo "Set BUCKET env var first, e.g. BUCKET=lsc4ss-wiki-attention-yourname"
    exit 1
fi

REGION="${REGION:-us-east-1}"
FUNCTION_NAME="${FUNCTION_NAME:-pageview-ingest}"
ROLE_NAME="${ROLE_NAME:-LabRole}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "==> Creating S3 bucket s3://${BUCKET} (region ${REGION})"
if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
    echo "    bucket already exists, skipping"
else
    if [[ "${REGION}" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
    else
        aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" \
            --create-bucket-configuration LocationConstraint="${REGION}"
    fi
fi

echo "==> Uploading config/articles.yaml"
aws s3 cp config/articles.yaml "s3://${BUCKET}/config/articles.yaml"

echo "==> Building Lambda deployment zip"
bash ingest/build_lambda_zip.sh

echo "==> Creating or updating Lambda function ${FUNCTION_NAME}"
if aws lambda get-function --function-name "${FUNCTION_NAME}" >/dev/null 2>&1; then
    echo "    function exists, updating code"
    aws lambda update-function-code \
        --function-name "${FUNCTION_NAME}" \
        --zip-file "fileb://ingest/build/pageview_ingest.zip" \
        --no-cli-pager
    aws lambda update-function-configuration \
        --function-name "${FUNCTION_NAME}" \
        --timeout 300 \
        --memory-size 1024 \
        --environment "Variables={BUCKET=${BUCKET},STAGING_PREFIX=staging,ARTICLES_KEY=config/articles.yaml}" \
        --no-cli-pager
else
    echo "    creating function"
    aws lambda create-function \
        --function-name "${FUNCTION_NAME}" \
        --runtime python3.11 \
        --role "${ROLE_ARN}" \
        --handler lambda_function.lambda_handler \
        --timeout 300 \
        --memory-size 1024 \
        --zip-file "fileb://ingest/build/pageview_ingest.zip" \
        --environment "Variables={BUCKET=${BUCKET},STAGING_PREFIX=staging,ARTICLES_KEY=config/articles.yaml}" \
        --no-cli-pager
fi

echo ""
echo "Done. Now smoke-test with one invocation:"
echo ""
echo "  aws lambda invoke \\"
echo "      --function-name ${FUNCTION_NAME} \\"
echo "      --cli-binary-format raw-in-base64-out \\"
echo "      --payload '{\"year\":2024,\"month\":1,\"day\":15,\"hour\":12}' \\"
echo "      response.json && cat response.json"
echo ""
echo "If that looks good, run the orchestrator:"
echo ""
echo "  python ingest/orchestrator.py --start 2019-01-01 --end 2026-05-01 \\"
echo "      --bucket ${BUCKET} --concurrency 40"
