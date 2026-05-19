#!/usr/bin/env bash
# Build a Lambda deployment zip containing lambda_function.py and its Python deps.
#
# Why a zip and not a container: Lambda zip deploys are simpler, faster to
# update, and avoid ECR cost. Our deps (pyarrow, pyyaml) compress to under
# the 50 MB direct-upload limit. boto3 is provided by the Lambda runtime so
# we don't bundle it.
#
# Run from the repo root:
#   bash ingest/build_lambda_zip.sh
# Output:
#   ingest/build/pageview_ingest.zip
#
# Then upload to Lambda either via Console (drag-drop into the function's
# Code source panel) or via:
#   aws lambda update-function-code \
#       --function-name pageview-ingest \
#       --zip-file fileb://ingest/build/pageview_ingest.zip

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
PKG_DIR="${BUILD_DIR}/pkg"
ZIP_PATH="${BUILD_DIR}/pageview_ingest.zip"

rm -rf "${BUILD_DIR}"
mkdir -p "${PKG_DIR}"

# Install pure-python and manylinux-compatible wheels into the package dir.
# We only need PyYAML; boto3 is provided by the Lambda runtime, and we
# deliberately avoid pyarrow to keep the zip well under the 50 MB direct
# upload limit. See lambda_function.py for the design note.
python3 -m pip install \
    --target "${PKG_DIR}" \
    --platform manylinux2014_x86_64 \
    --python-version 3.11 \
    --only-binary=:all: \
    --upgrade \
    pyyaml==6.0.1

# Drop dist-info bloat to stay under the Lambda zip limits.
find "${PKG_DIR}" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "${PKG_DIR}" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true

# Copy the handler in last so it ends up at the zip root.
cp "${SCRIPT_DIR}/lambda_function.py" "${PKG_DIR}/lambda_function.py"

# Zip up the contents of pkg/, NOT the pkg/ dir itself — Lambda needs the
# handler at the root.
(cd "${PKG_DIR}" && zip -qr "${ZIP_PATH}" .)

echo ""
echo "Built: ${ZIP_PATH}"
ls -lh "${ZIP_PATH}"
echo ""
echo "Next: upload to Lambda function 'pageview-ingest' via Console"
echo "or:   aws lambda update-function-code --function-name pageview-ingest \\"
echo "          --zip-file fileb://${ZIP_PATH}"
