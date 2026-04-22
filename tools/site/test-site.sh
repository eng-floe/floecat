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
JEKYLL_PAGES_IMAGE="${JEKYLL_PAGES_IMAGE:-jekyll/jekyll:pages}"
JEKYLL_ENVIRONMENT="${JEKYLL_ENV:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required for test-site"
  exit 1
fi

echo "==> [SITE] building website with Jekyll (${JEKYLL_PAGES_IMAGE})"
docker run --rm \
  -e "JEKYLL_UID=$(id -u)" \
  -e "JEKYLL_GID=$(id -g)" \
  ${JEKYLL_ENVIRONMENT:+-e "JEKYLL_ENV=${JEKYLL_ENVIRONMENT}"} \
  -v "${SITE_DIR}:/srv/jekyll" \
  "${JEKYLL_PAGES_IMAGE}" \
  jekyll build

echo "==> [SITE] checking expected output files"
test -f "${SITE_OUT_DIR}/index.html" || { echo "missing ${SITE_OUT_DIR}/index.html"; exit 1; }
test -f "${SITE_OUT_DIR}/blog/index.html" || { echo "missing ${SITE_OUT_DIR}/blog/index.html"; exit 1; }
test -f "${SITE_OUT_DIR}/robots.txt" || { echo "missing ${SITE_OUT_DIR}/robots.txt"; exit 1; }
echo "==> [SITE] basic checks passed"
