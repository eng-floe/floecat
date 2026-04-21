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

if [[ ! -f "${SITE_OUT_DIR}/index.html" ]]; then
  echo "ERROR: ${SITE_OUT_DIR}/index.html missing. Run 'make test-site' first."
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

npm exec --yes --package=playwright@1.49.1 -- \
  playwright screenshot \
  --browser chromium \
  "${PLAYWRIGHT_CHANNEL_ARGS[@]}" \
  --timeout 45000 \
  --wait-for-selector "#main" \
  "${BASE_URL}" \
  "${PREVIEW_ROOT}/home.png"

npm exec --yes --package=playwright@1.49.1 -- \
  playwright screenshot \
  --browser chromium \
  "${PLAYWRIGHT_CHANNEL_ARGS[@]}" \
  --timeout 45000 \
  --wait-for-selector "#main" \
  "${BASE_URL}blog/" \
  "${PREVIEW_ROOT}/blog.png"

test -s "${PREVIEW_ROOT}/home.png" || { echo "missing ${PREVIEW_ROOT}/home.png"; exit 1; }
test -s "${PREVIEW_ROOT}/blog.png" || { echo "missing ${PREVIEW_ROOT}/blog.png"; exit 1; }

echo "==> [SITE-E2E] smoke checks passed"
