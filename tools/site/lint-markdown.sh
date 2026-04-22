#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

if ! command -v npx >/dev/null 2>&1; then
  echo "ERROR: npx is required for markdown linting"
  exit 1
fi

# Keep version pinned so local and CI lint behavior stay consistent.
npx --yes markdownlint-cli2@0.18.1
