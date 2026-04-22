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
PREVIEW_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/floecat-preview.XXXXXX")"
HOST="${SITE_PREVIEW_HOST:-127.0.0.1}"
PORT="${SITE_PREVIEW_PORT:-4000}"
SKIP_BUILD=0

usage() {
  cat <<'EOF'
Usage:
  serve-site.sh [--port <port>] [--host <host>] [--skip-build]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    --host)
      HOST="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required for site preview"
  exit 1
fi

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && ps -p "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${PREVIEW_ROOT}"
}
trap cleanup EXIT

if [[ "${SKIP_BUILD}" -ne 1 ]]; then
  echo "==> [SITE-PREVIEW] building site + docs first"
  "${ROOT_DIR}/tools/site/test-site.sh"
fi

if [[ ! -f "${SITE_OUT_DIR}/index.html" ]]; then
  echo "ERROR: ${SITE_OUT_DIR}/index.html missing. Run 'make test-site' first."
  exit 1
fi

mkdir -p "${PREVIEW_ROOT}/floecat"
cp -a "${SITE_OUT_DIR}/." "${PREVIEW_ROOT}/floecat/"

echo "==> [SITE-PREVIEW] serving:"
echo "    http://${HOST}:${PORT}/floecat/"
echo "    http://${HOST}:${PORT}/floecat/documentation/"
echo "==> [SITE-PREVIEW] press Ctrl+C to stop"

python3 -m http.server "${PORT}" --bind "${HOST}" --directory "${PREVIEW_ROOT}" >/dev/null 2>&1 &
SERVER_PID="$!"
wait "${SERVER_PID}"
