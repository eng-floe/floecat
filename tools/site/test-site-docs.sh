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
SITE_OUT_DIR="${ROOT_DIR}/site-src/_site"
DOCS_OUT_DIR="${SITE_OUT_DIR}/documentation"

echo "==> [DOCS] building repository docs under /documentation/"
"${ROOT_DIR}/tools/site/build-docs.sh" --output-dir "${DOCS_OUT_DIR}"

echo "==> [DOCS] checking expected docs output files"
test -f "${DOCS_OUT_DIR}/index.html" || { echo "missing ${DOCS_OUT_DIR}/index.html"; exit 1; }
echo "==> [DOCS] docs checks passed"
