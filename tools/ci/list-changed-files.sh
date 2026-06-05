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

# Print changed files for CI routing. PR runs compare the PR base commit to the
# actual PR head commit, not GitHub's synthetic merge commit, so shallow clones
# do not need a merge base.
#
# Required env:
#   EVENT_NAME
#   HEAD_SHA
#
# Optional env:
#   BASE_SHA    (pull_request base sha)
#   BEFORE_SHA  (push before sha)
#   PR_NUMBER   (pull_request number; used as fallback for fetching PR heads)

EVENT_NAME="${EVENT_NAME:-}"
BASE_SHA="${BASE_SHA:-}"
BEFORE_SHA="${BEFORE_SHA:-}"
HEAD_SHA="${HEAD_SHA:-}"
PR_NUMBER="${PR_NUMBER:-}"

fetch_commit() {
  local sha="$1"
  [[ -n "${sha}" ]] || return 1
  git fetch --no-tags --depth=1 origin "${sha}" >/dev/null 2>&1 || true
  git cat-file -e "${sha}^{commit}" >/dev/null 2>&1
}

if [[ "${EVENT_NAME}" == "pull_request" && -n "${BASE_SHA}" && -n "${HEAD_SHA}" ]]; then
  if ! fetch_commit "${HEAD_SHA}" && [[ -n "${PR_NUMBER}" ]]; then
    git fetch --no-tags --depth=1 origin "pull/${PR_NUMBER}/head" >/dev/null 2>&1 || true
  fi
  if fetch_commit "${BASE_SHA}" && fetch_commit "${HEAD_SHA}"; then
    git diff --name-only "${BASE_SHA}" "${HEAD_SHA}"
  fi
elif [[ "${EVENT_NAME}" == "push" && -n "${BEFORE_SHA}" && -n "${HEAD_SHA}" && "${BEFORE_SHA}" != "0000000000000000000000000000000000000000" ]]; then
  if fetch_commit "${BEFORE_SHA}" && fetch_commit "${HEAD_SHA}"; then
    git diff --name-only "${BEFORE_SHA}" "${HEAD_SHA}"
  fi
fi
