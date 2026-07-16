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

# Print changed files for CI routing. PR runs compare the whole PR branch to
# its merge base with the target branch, using the actual PR head commit rather
# than GitHub's synthetic merge commit.
#
# Required env:
#   EVENT_NAME
#   HEAD_SHA
#
# Optional env:
#   BASE_SHA    (pull_request base sha)
#   BEFORE_SHA  (push before sha)
#   PR_NUMBER   (pull_request number; used as fallback for fetching PR heads)
#   CHANGED_FILES_FETCH_DEPTH      (initial PR history fetch depth; default 128)
#   CHANGED_FILES_MAX_FETCH_DEPTH  (maximum PR history fetch depth; default 4096)

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

fetch_pr_history() {
  local depth="$1"

  git fetch --no-tags --depth="${depth}" origin "${BASE_SHA}" >/dev/null 2>&1 || true
  if [[ -n "${PR_NUMBER}" ]]; then
    git fetch --no-tags --depth="${depth}" origin "pull/${PR_NUMBER}/head" >/dev/null 2>&1 || true
  fi
  git fetch --no-tags --depth="${depth}" origin "${HEAD_SHA}" >/dev/null 2>&1 || true

  git cat-file -e "${BASE_SHA}^{commit}" >/dev/null 2>&1 &&
    git cat-file -e "${HEAD_SHA}^{commit}" >/dev/null 2>&1
}

if [[ "${EVENT_NAME}" == "pull_request" && -n "${BASE_SHA}" && -n "${HEAD_SHA}" ]]; then
  depth="${CHANGED_FILES_FETCH_DEPTH:-128}"
  max_depth="${CHANGED_FILES_MAX_FETCH_DEPTH:-4096}"
  merge_base=""

  while [[ "${depth}" -le "${max_depth}" ]]; do
    if fetch_pr_history "${depth}"; then
      merge_base="$(git merge-base "${BASE_SHA}" "${HEAD_SHA}" 2>/dev/null || true)"
      if [[ -n "${merge_base}" ]]; then
        git diff --name-only "${merge_base}" "${HEAD_SHA}"
        exit 0
      fi
    fi

    if [[ "${depth}" -eq "${max_depth}" ]]; then
      break
    fi
    depth=$((depth * 2))
    if [[ "${depth}" -gt "${max_depth}" ]]; then
      depth="${max_depth}"
    fi
  done

  echo "ERROR: unable to find merge base for PR base ${BASE_SHA} and head ${HEAD_SHA}" >&2
  exit 1
elif [[ "${EVENT_NAME}" == "push" && -n "${BEFORE_SHA}" && -n "${HEAD_SHA}" && "${BEFORE_SHA}" != "0000000000000000000000000000000000000000" ]]; then
  if ! fetch_commit "${BEFORE_SHA}"; then
    echo "ERROR: unable to fetch push base commit ${BEFORE_SHA}" >&2
    exit 1
  fi

  if ! fetch_commit "${HEAD_SHA}"; then
    echo "ERROR: unable to fetch push head commit ${HEAD_SHA}" >&2
    exit 1
  fi

  git diff --name-only "${BEFORE_SHA}" "${HEAD_SHA}"
fi
