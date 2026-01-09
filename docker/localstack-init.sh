#!/usr/bin/env bash
set -euo pipefail

REGION="${FLOECAT_S3_REGION:-us-east-1}"
BUCKET="${FLOECAT_BLOB_S3_BUCKET:-floecat-dev}"
TABLE="${FLOECAT_KV_TABLE:-floecat_pointers}"

awslocal s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" >/dev/null 2>&1 || true
awslocal s3api create-bucket --bucket "bucket" --region "${REGION}" >/dev/null 2>&1 || true
awslocal s3api create-bucket --bucket "warehouse" --region "${REGION}" >/dev/null 2>&1 || true
awslocal s3api create-bucket --bucket "yb-iceberg-tpcds" --region "${REGION}" >/dev/null 2>&1 || true
awslocal s3api create-bucket --bucket "floecat" --region "${REGION}" >/dev/null 2>&1 || true
awslocal s3api create-bucket --bucket "staged-fixtures" --region "${REGION}" >/dev/null 2>&1 || true
awslocal s3api create-bucket --bucket "floecat-delta" --region "${REGION}" >/dev/null 2>&1 || true

awslocal dynamodb create-table \
  --table-name "${TABLE}" \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST >/dev/null 2>&1 || true

awslocal dynamodb update-time-to-live \
  --table-name "${TABLE}" \
  --time-to-live-specification '{"Enabled":true,"AttributeName":"expires_at"}' >/dev/null 2>&1 || true
