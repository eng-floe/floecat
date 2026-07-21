#!/usr/bin/env bash
set -euo pipefail

REGION="${FLOECAT_STORAGE_AWS_REGION:-us-east-1}"
BUCKET="${FLOECAT_BLOB_S3_BUCKET:-floecat-dev}"
TABLE="${FLOECAT_KV_TABLE:-floecat_pointers}"

awslocal s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" >/dev/null 2>&1 || true
# CAS blob GC fails closed (collects nothing) unless the blob bucket's versioning is Enabled:
# version-targeted deletes are what let a delete race a concurrent re-reference safely.
awslocal s3api put-bucket-versioning --bucket "${BUCKET}" \
  --versioning-configuration Status=Enabled >/dev/null 2>&1 || true
# Every content-addressed re-reference is deliberately a fresh PUT so a concurrent CAS GC cannot
# delete it. Versioning turns those PUTs into noncurrent versions; expire old history so hot keys do
# not grow without bound. Keep a small, time-bounded recovery window, matching the production
# prerequisite documented in docs/storage-aws.md.
awslocal s3api put-bucket-lifecycle-configuration --bucket "${BUCKET}" \
  --lifecycle-configuration \
  '{"Rules":[{"ID":"floecat-expire-noncurrent-cas-versions","Status":"Enabled","Filter":{"Prefix":""},"NoncurrentVersionExpiration":{"NoncurrentDays":30,"NewerNoncurrentVersions":3}}]}' \
  >/dev/null 2>&1 || true
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
