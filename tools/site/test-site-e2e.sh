#!/usr/bin/env bash
# Copyright 2026 Yellowbrick Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SITE_DIR="${ROOT_DIR}/site-src"
SITE_OUT_DIR="${SITE_DIR}/_site"
PREVIEW_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/floecat-e2e.XXXXXX")"
PORT="${SITE_E2E_PORT:-8084}"
HOST="${SITE_E2E_HOST:-127.0.0.1}"
BASE_URL="http://${HOST}:${PORT}/floecat/"
PATHS_FILE="${SITE_E2E_PATHS_FILE:-}"
SERVER_LOG="${PREVIEW_ROOT}/http-server.log"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && ps -p "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${PREVIEW_ROOT}"
}
trap cleanup EXIT

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required for test-site-e2e"
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "ERROR: npm is required for test-site-e2e"
  exit 1
fi

if [[ ! -f "${SITE_OUT_DIR}/index.html" && ! -f "${SITE_OUT_DIR}/documentation/index.html" ]]; then
  echo "ERROR: expected site output missing (need index.html or documentation/index.html). Run 'make test-site' or area-specific build first."
  exit 1
fi

mkdir -p "${PREVIEW_ROOT}/floecat"
cp -a "${SITE_OUT_DIR}/." "${PREVIEW_ROOT}/floecat/"

echo "==> [SITE-E2E] serving preview from ${BASE_URL}"
python3 -m http.server "${PORT}" --bind "${HOST}" --directory "${PREVIEW_ROOT}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID="$!"

for _ in $(seq 1 30); do
  if curl -fsS "${BASE_URL}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS "${BASE_URL}" >/dev/null 2>&1; then
  echo "ERROR: preview server did not become ready"
  cat "${SERVER_LOG}" || true
  exit 1
fi

echo "==> [SITE-E2E] running Playwright smoke checks"
PLAYWRIGHT_CHANNEL="${PLAYWRIGHT_CHANNEL:-chrome}"
PLAYWRIGHT_CHANNEL_ARGS=()
if [[ -n "${PLAYWRIGHT_CHANNEL}" ]]; then
  PLAYWRIGHT_CHANNEL_ARGS=(--channel "${PLAYWRIGHT_CHANNEL}")
fi

PATHS=()
if [[ -n "${PATHS_FILE}" && -f "${PATHS_FILE}" ]]; then
  while IFS= read -r path; do
    path="$(echo "${path}" | tr -d '\r')"
    [[ -z "${path}" ]] && continue
    PATHS+=("${path}")
  done < "${PATHS_FILE}"
fi

if [[ "${#PATHS[@]}" -eq 0 ]]; then
  PATHS=("/floecat/" "/floecat/blog/" "/floecat/documentation/")
fi

PASS_COUNT=0
for path in "${PATHS[@]}"; do
  url="http://${HOST}:${PORT}${path}"
  if [[ "${path}" == "/" ]]; then
    safe_name="root"
  else
    safe_name="$(echo "${path}" | sed -E 's#^/##; s#/$##; s#[^a-zA-Z0-9._-]+#-#g')"
  fi
  output_png="${PREVIEW_ROOT}/${safe_name}.png"

  npm exec --yes --package=playwright@1.49.1 -- \
    playwright screenshot \
    --browser chromium \
    "${PLAYWRIGHT_CHANNEL_ARGS[@]}" \
    --timeout 45000 \
    --wait-for-selector "#main, .md-main, main" \
    "${url}" \
    "${output_png}"

  test -s "${output_png}" || { echo "missing ${output_png}"; exit 1; }
  PASS_COUNT=$((PASS_COUNT + 1))
done

echo "==> [SITE-E2E] validated ${PASS_COUNT} page(s)"

echo "==> [SITE-E2E] smoke checks passed"
