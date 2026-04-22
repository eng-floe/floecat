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
DOCS_OUT_DIR="${ROOT_DIR}/site-src/_site/documentation"
MKDOCS_IMAGE="${MKDOCS_IMAGE:-squidfunk/mkdocs-material:9.6.14}"
# Optional override; if empty we derive a branch-aware default for edit links.
DOCS_EDIT_URI="${DOCS_EDIT_URI:-}"

usage() {
  cat <<'EOF'
Usage:
  build-docs.sh [--output-dir <path>]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      DOCS_OUT_DIR="${2:-}"
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

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required for docs build"
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/mkdocs.yml" ]]; then
  echo "ERROR: mkdocs.yml not found at repo root"
  exit 1
fi

if [[ -z "${DOCS_EDIT_URI}" ]]; then
  # Prefer CI branch context when available so PR preview edit links do not
  # point to main and produce 404s for branch-only docs.
  branch="${DOCS_EDIT_BRANCH:-}"
  if [[ -z "${branch}" && -n "${GITHUB_HEAD_REF:-}" ]]; then
    branch="${GITHUB_HEAD_REF}"
  fi
  if [[ -z "${branch}" && -n "${GITHUB_REF_NAME:-}" ]]; then
    branch="${GITHUB_REF_NAME}"
  fi
  if [[ -z "${branch}" ]]; then
    branch="$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  fi
  if [[ -z "${branch}" || "${branch}" == "HEAD" ]]; then
    branch="main"
  fi
  DOCS_EDIT_URI="https://github.com/eng-floe/floecat/edit/${branch}/docs/"
fi

rm -rf "${DOCS_OUT_DIR}"
mkdir -p "${DOCS_OUT_DIR}"

echo "==> [DOCS] building docs with MkDocs Material (${MKDOCS_IMAGE})"
echo "==> [DOCS] edit links target: ${DOCS_EDIT_URI}"
docker run --rm \
  -u "$(id -u):$(id -g)" \
  -w /work \
  -e "DOCS_EDIT_URI=${DOCS_EDIT_URI}" \
  -v "${ROOT_DIR}:/work" \
  -v "${DOCS_OUT_DIR}:/out" \
  "${MKDOCS_IMAGE}" \
  build \
  --clean \
  --config-file /work/mkdocs.yml \
  --site-dir /out

test -f "${DOCS_OUT_DIR}/index.html" || {
  echo "ERROR: docs build output missing ${DOCS_OUT_DIR}/index.html"
  exit 1
}
echo "==> [DOCS] docs build passed"
