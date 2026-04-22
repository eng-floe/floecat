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

# Shared change detection for workflows that should skip heavy core checks when
# a PR/push only touches website/docs files.
#
# Required env:
#   EVENT_NAME  (github.event_name)
#   HEAD_SHA    (github.sha)
#
# Optional env:
#   BASE_SHA    (github.event.pull_request.base.sha)
#   BEFORE_SHA  (github.event.before)
#   CORE_NON_CORE_REGEX (override default non-core matcher)
#
# Output:
#   core_changed=true|false written to $GITHUB_OUTPUT when available.

EVENT_NAME="${EVENT_NAME:-}"
BASE_SHA="${BASE_SHA:-}"
BEFORE_SHA="${BEFORE_SHA:-}"
HEAD_SHA="${HEAD_SHA:-}"
NON_CORE_RE="${CORE_NON_CORE_REGEX:-^(site-src/|docs/|mkdocs\.yml$|\.markdownlint-cli2\.yaml$|lighthouserc\.json$|tools/site/|\.github/workflows/pages\.yml$)}"

core_changed=true
changed_files=""

if [[ "${EVENT_NAME}" == "pull_request" && -n "${BASE_SHA}" ]]; then
  git fetch --no-tags --depth=1 origin "${BASE_SHA}" || true
  if git cat-file -e "${BASE_SHA}^{commit}" >/dev/null 2>&1; then
    changed_files="$(git diff --name-only "${BASE_SHA}...${HEAD_SHA}" || true)"
  fi
elif [[ "${EVENT_NAME}" == "push" && -n "${BEFORE_SHA}" && "${BEFORE_SHA}" != "0000000000000000000000000000000000000000" ]]; then
  git fetch --no-tags --depth=1 origin "${BEFORE_SHA}" || true
  if git cat-file -e "${BEFORE_SHA}^{commit}" >/dev/null 2>&1; then
    changed_files="$(git diff --name-only "${BEFORE_SHA}...${HEAD_SHA}" || true)"
  fi
fi

if [[ -n "${changed_files}" ]]; then
  if echo "${changed_files}" | grep -qEv "${NON_CORE_RE}"; then
    core_changed=true
  else
    core_changed=false
  fi
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "core_changed=${core_changed}" >> "${GITHUB_OUTPUT}"
fi
echo "Detected core_changed=${core_changed}"
