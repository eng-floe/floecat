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
SERVER_LOG="${PREVIEW_ROOT}/http-server.log"
HOST="${SITE_PREVIEW_HOST:-127.0.0.1}"
PORT="${SITE_PREVIEW_PORT:-4000}"
SKIP_BUILD=0
WATCH=1
POLL_SECONDS="${SITE_PREVIEW_POLL_SECONDS:-2}"

usage() {
  cat <<'EOF'
Usage:
  serve-site.sh [--port <port>] [--host <host>] [--skip-build] [--watch|--no-watch] [--poll-seconds <n>]
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
    --watch)
      WATCH=1
      shift
      ;;
    --no-watch)
      WATCH=0
      shift
      ;;
    --poll-seconds)
      POLL_SECONDS="${2:-}"
      shift 2
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

if [[ "${WATCH}" -eq 1 ]] && ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required for watch mode (rebuilds use test-site)"
  exit 1
fi

if ! [[ "${POLL_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${POLL_SECONDS}" -lt 1 ]]; then
  echo "ERROR: --poll-seconds must be an integer >= 1"
  exit 1
fi

choose_hasher() {
  if command -v sha256sum >/dev/null 2>&1; then
    HASH_CMD=(sha256sum)
  elif command -v shasum >/dev/null 2>&1; then
    HASH_CMD=(shasum -a 256)
  elif command -v sha1sum >/dev/null 2>&1; then
    HASH_CMD=(sha1sum)
  else
    HASH_CMD=(cksum)
  fi
}

build_sources_fingerprint() {
  local manifest tmp
  tmp="$(mktemp "${TMPDIR:-/tmp}/floecat-preview-manifest.XXXXXX")"
  while IFS= read -r path; do
    [[ -f "${path}" ]] || continue
    "${HASH_CMD[@]}" "${path}" >> "${tmp}"
  done < <(
    {
      find "${SITE_DIR}" -type f ! -path "${SITE_OUT_DIR}/*" 2>/dev/null
      find "${ROOT_DIR}/docs" -type f 2>/dev/null
      echo "${ROOT_DIR}/mkdocs.yml"
    } | LC_ALL=C sort
  )
  manifest="$("${HASH_CMD[@]}" "${tmp}" | awk '{print $1}')"
  rm -f "${tmp}"
  echo "${manifest}"
}

sync_preview_root() {
  local next_dir old_dir
  next_dir="${PREVIEW_ROOT}/floecat.next"
  old_dir="${PREVIEW_ROOT}/floecat.prev"

  rm -rf "${next_dir}" "${old_dir}"
  mkdir -p "${next_dir}"
  cp -a "${SITE_OUT_DIR}/." "${next_dir}/"

  if [[ -d "${PREVIEW_ROOT}/floecat" ]]; then
    mv "${PREVIEW_ROOT}/floecat" "${old_dir}"
  fi
  mv "${next_dir}" "${PREVIEW_ROOT}/floecat"
  rm -rf "${old_dir}"
}

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

choose_hasher
sync_preview_root

echo "==> [SITE-PREVIEW] serving:"
echo "    http://${HOST}:${PORT}/floecat/"
echo "    http://${HOST}:${PORT}/floecat/documentation/"
echo "==> [SITE-PREVIEW] press Ctrl+C to stop"

python3 -m http.server "${PORT}" --bind "${HOST}" --directory "${PREVIEW_ROOT}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID="$!"
sleep 1
if ! ps -p "${SERVER_PID}" >/dev/null 2>&1; then
  echo "ERROR: failed to start preview server"
  if [[ -f "${SERVER_LOG}" ]]; then
    tail -n 40 "${SERVER_LOG}" || true
  fi
  exit 1
fi

if [[ "${WATCH}" -eq 1 ]]; then
  echo "==> [SITE-PREVIEW] watching for changes in site-src/, docs/, mkdocs.yml (poll ${POLL_SECONDS}s)"
  last_fingerprint="$(build_sources_fingerprint)"
  while true; do
    sleep "${POLL_SECONDS}"
    if ! ps -p "${SERVER_PID}" >/dev/null 2>&1; then
      echo "ERROR: preview server exited unexpectedly"
      if [[ -f "${SERVER_LOG}" ]]; then
        tail -n 40 "${SERVER_LOG}" || true
      fi
      exit 1
    fi
    current_fingerprint="$(build_sources_fingerprint)"
    if [[ "${current_fingerprint}" != "${last_fingerprint}" ]]; then
      echo "==> [SITE-PREVIEW] change detected, rebuilding..."
      if "${ROOT_DIR}/tools/site/test-site.sh"; then
        sync_preview_root
        echo "==> [SITE-PREVIEW] updated. Refresh your browser."
      else
        echo "==> [SITE-PREVIEW] rebuild failed; keeping previous preview output."
      fi
      last_fingerprint="${current_fingerprint}"
    fi
  done
else
  wait "${SERVER_PID}"
fi
