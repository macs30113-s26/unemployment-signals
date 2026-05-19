<#
.SYNOPSIS
    AWS resource setup for the pageview ingest pipeline (Windows / PowerShell version).

.DESCRIPTION
    PowerShell port of setup_aws.sh. Same behavior:
      - Creates the S3 bucket (idempotent)
      - Uploads articles.yaml to s3://$Bucket/config/
      - Builds the Lambda zip (using Compress-Archive instead of zip)
      - Creates the Lambda function 'pageview-ingest' or updates code if it exists

.PARAMETER Bucket
    Globally-unique S3 bucket name.

.PARAMETER Region
    AWS region (default us-east-1).

.PARAMETER FunctionName
    Lambda function name (default pageview-ingest).

.EXAMPLE
    .\setup_aws.ps1 -Bucket lsc4ss-wiki-attention-henry-2026

.NOTES
    Run this AFTER:
      1. Starting the AWS Academy Learner Lab
      2. Running 'aws configure' with the lab credentials
    All resources are created in the region you set (default us-east-1).
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$Bucket,

    [string]$Region = "us-east-1",
    [string]$FunctionName = "pageview-ingest",
    [string]$RoleName = "LabRole"
)

$ErrorActionPreference = "Stop"

# Move into the repo root (this script lives there)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# Get the account ID — used to build the LabRole ARN.
Write-Host "==> Looking up AWS account ID"
$AccountId = (aws sts get-caller-identity --query Account --output text).Trim()
if ([string]::IsNullOrEmpty($AccountId)) {
    Write-Error "Could not get AWS account ID. Is 'aws configure' set up with valid credentials?"
    exit 1
}
$RoleArn = "arn:aws:iam::${AccountId}:role/${RoleName}"
Write-Host "    account: $AccountId"
Write-Host "    role:    $RoleArn"

# ---------- S3 bucket ----------
Write-Host ""
Write-Host "==> Creating S3 bucket s3://$Bucket (region $Region)"
$bucketExists = $false
try {
    aws s3api head-bucket --bucket $Bucket 2>$null
    if ($LASTEXITCODE -eq 0) { $bucketExists = $true }
} catch {}

if ($bucketExists) {
    Write-Host "    bucket already exists, skipping create"
} else {
    if ($Region -eq "us-east-1") {
        aws s3api create-bucket --bucket $Bucket --region $Region | Out-Null
    } else {
        aws s3api create-bucket --bucket $Bucket --region $Region `
            --create-bucket-configuration LocationConstraint=$Region | Out-Null
    }
    Write-Host "    bucket created"
}

# ---------- upload articles.yaml ----------
Write-Host ""
Write-Host "==> Uploading config/articles.yaml"
aws s3 cp "config/articles.yaml" "s3://$Bucket/config/articles.yaml" | Out-Null

# ---------- build Lambda zip ----------
Write-Host ""
Write-Host "==> Building Lambda deployment zip"
$BuildDir = Join-Path $ScriptDir "ingest\build"
$PkgDir = Join-Path $BuildDir "pkg"
$ZipPath = Join-Path $BuildDir "pageview_ingest.zip"

if (Test-Path $BuildDir) { Remove-Item -Recurse -Force $BuildDir }
New-Item -ItemType Directory -Force -Path $PkgDir | Out-Null

# Install PyYAML targeting Linux x86_64 manylinux wheels (Lambda's runtime),
# even though we're on Windows. The --platform / --only-binary flags force
# pip to download platform-correct wheels rather than build from source.
Write-Host "    installing PyYAML for Lambda runtime (Linux x86_64)"
python -m pip install `
    --target $PkgDir `
    --platform manylinux2014_x86_64 `
    --python-version 3.11 `
    --only-binary=:all: `
    --upgrade `
    pyyaml==6.0.1 | Out-Null

# Drop unnecessary stuff
Get-ChildItem -Path $PkgDir -Filter "__pycache__" -Recurse -Force -Directory `
    | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path $PkgDir -Filter "tests" -Recurse -Force -Directory `
    | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Copy the handler to the package root
Copy-Item "ingest\lambda_function.py" -Destination (Join-Path $PkgDir "lambda_function.py")

# Zip — note the contents of pkg/, not pkg/ itself, must be at the zip root.
# Compress-Archive on a wildcard achieves this.
Write-Host "    creating zip"
Compress-Archive -Path (Join-Path $PkgDir "*") -DestinationPath $ZipPath -Force

$zipSize = (Get-Item $ZipPath).Length / 1KB
Write-Host "    built: $ZipPath ($([math]::Round($zipSize, 1)) KB)"

# ---------- Lambda function ----------
Write-Host ""
Write-Host "==> Creating or updating Lambda function $FunctionName"
$EnvVars = "Variables={BUCKET=$Bucket,STAGING_PREFIX=staging,ARTICLES_KEY=config/articles.yaml}"

$functionExists = $false
try {
    aws lambda get-function --function-name $FunctionName 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $functionExists = $true }
} catch {}

if ($functionExists) {
    Write-Host "    function exists, updating code + configuration"
    aws lambda update-function-code `
        --function-name $FunctionName `
        --zip-file "fileb://$ZipPath" `
        --no-cli-pager | Out-Null

    # Lambda needs a beat between code update and config update
    Start-Sleep -Seconds 5

    aws lambda update-function-configuration `
        --function-name $FunctionName `
        --timeout 300 `
        --memory-size 1024 `
        --environment $EnvVars `
        --no-cli-pager | Out-Null
} else {
    Write-Host "    creating new function"
    aws lambda create-function `
        --function-name $FunctionName `
        --runtime python3.11 `
        --role $RoleArn `
        --handler lambda_function.lambda_handler `
        --timeout 300 `
        --memory-size 1024 `
        --zip-file "fileb://$ZipPath" `
        --environment $EnvVars `
        --no-cli-pager | Out-Null
}

Write-Host ""
Write-Host "===== Setup complete ====="
Write-Host ""
Write-Host "Smoke-test the Lambda on one hour:"
Write-Host ""
Write-Host "  aws lambda invoke --function-name $FunctionName ``"
Write-Host "      --cli-binary-format raw-in-base64-out ``"
Write-Host "      --payload '{\""year\"":2024,\""month\"":1,\""day\"":15,\""hour\"":12}' ``"
Write-Host "      response.json"
Write-Host "  Get-Content response.json"
Write-Host ""
Write-Host "If match_count > 0, fan out the full ingest:"
Write-Host ""
Write-Host "  python ingest/orchestrator.py --start 2019-01-01 --end 2026-05-01 ``"
Write-Host "      --bucket $Bucket --concurrency 40"
