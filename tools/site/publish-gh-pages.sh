#!/usr/bin/env bash
# Publish production and PR preview website content to the gh-pages branch.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  publish-gh-pages.sh deploy-prod --source-dir <dir>
  publish-gh-pages.sh deploy-pr --source-dir <dir> --pr-number <number>
  publish-gh-pages.sh remove-pr --pr-number <number>

Required env:
  GITHUB_TOKEN   GitHub token with contents:write
  REPO           owner/name (for example eng-floe/floecat)
Optional env:
  SOURCE_SHA     commit sha for deploy commit message
EOF
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "ERROR: missing required env var: ${name}" >&2
    exit 1
  fi
}

parse_args() {
  CMD="${1:-}"
  shift || true
  SOURCE_DIR=""
  PR_NUMBER=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --source-dir)
        SOURCE_DIR="${2:-}"
        shift 2
        ;;
      --pr-number)
        PR_NUMBER="${2:-}"
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
}

init_branch_workdir() {
  local auth_header
  BRANCH_ROOT="$(mktemp -d /tmp/floecat-gh-pages.XXXXXX)"
  git init -b gh-pages "${BRANCH_ROOT}" >/dev/null
  pushd "${BRANCH_ROOT}" >/dev/null
  git config user.name "github-actions[bot]"
  git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
  auth_header="$(printf 'x-access-token:%s' "${GITHUB_TOKEN}" | base64 | tr -d '\n')"
  git config "http.https://github.com/.extraheader" "AUTHORIZATION: basic ${auth_header}"
  git remote add origin "https://github.com/${REPO}.git"

  if git fetch origin gh-pages >/dev/null 2>&1; then
    git checkout -B gh-pages FETCH_HEAD >/dev/null
  else
    git checkout --orphan gh-pages >/dev/null
    find . -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
  fi
}

cleanup() {
  if [[ -n "${BRANCH_ROOT:-}" ]] && [[ "$(pwd)" == "${BRANCH_ROOT}"* ]]; then
    popd >/dev/null || true
  fi
  if [[ -n "${BRANCH_ROOT:-}" && -d "${BRANCH_ROOT}" ]]; then
    rm -rf "${BRANCH_ROOT}"
  fi
}

commit_and_push_if_changed() {
  local message="$1"
  local push_output
  if [[ -n "$(git status --porcelain)" ]]; then
    git add -A
    git commit -m "${message}" >/dev/null
    local attempt
    for attempt in 1 2 3 4 5; do
      push_output="$(GIT_TERMINAL_PROMPT=0 git push origin gh-pages 2>&1)" && {
        echo "Pushed gh-pages update: ${message}"
        return
      }

      if ! grep -Eq 'non-fast-forward|fetch first|\[rejected\]' <<<"${push_output}"; then
        echo "ERROR: failed to push gh-pages (attempt ${attempt})." >&2
        echo "${push_output}" >&2
        exit 1
      fi

      echo "gh-pages push conflict (attempt ${attempt}), rebasing and retrying..." >&2
      if ! GIT_TERMINAL_PROMPT=0 git fetch origin gh-pages >/dev/null 2>&1; then
        echo "ERROR: failed to fetch gh-pages during retry." >&2
        exit 1
      fi
      if ! git rebase origin/gh-pages >/dev/null 2>&1; then
        git rebase --abort >/dev/null 2>&1 || true
        echo "ERROR: failed to rebase on latest gh-pages during retry." >&2
        exit 1
      fi
    done

    echo "ERROR: failed to push gh-pages after multiple retries." >&2
    exit 1
  else
    echo "No gh-pages changes to push."
  fi
}

deploy_prod() {
  if [[ -z "${SOURCE_DIR}" ]]; then
    echo "ERROR: --source-dir is required for deploy-prod" >&2
    exit 1
  fi
  if [[ ! -d "${SOURCE_DIR}" ]]; then
    echo "ERROR: source directory does not exist: ${SOURCE_DIR}" >&2
    exit 1
  fi

  # Keep PR previews but replace production root content.
  find . -mindepth 1 -maxdepth 1 ! -name .git ! -name pr-preview -exec rm -rf {} +
  cp -a "${SOURCE_DIR}/." .
  touch .nojekyll
  commit_and_push_if_changed "pages: publish main ${SOURCE_SHA:-unknown}"
}

deploy_pr() {
  if [[ -z "${SOURCE_DIR}" || -z "${PR_NUMBER}" ]]; then
    echo "ERROR: --source-dir and --pr-number are required for deploy-pr" >&2
    exit 1
  fi
  if [[ ! -d "${SOURCE_DIR}" ]]; then
    echo "ERROR: source directory does not exist: ${SOURCE_DIR}" >&2
    exit 1
  fi
  local target="pr-preview/pr-${PR_NUMBER}"
  rm -rf "${target}"
  mkdir -p "$(dirname "${target}")"
  cp -a "${SOURCE_DIR}" "${target}"
  touch .nojekyll
  commit_and_push_if_changed "preview: update PR #${PR_NUMBER}"
}

remove_pr() {
  if [[ -z "${PR_NUMBER}" ]]; then
    echo "ERROR: --pr-number is required for remove-pr" >&2
    exit 1
  fi
  local target="pr-preview/pr-${PR_NUMBER}"
  if [[ -e "${target}" ]]; then
    rm -rf "${target}"
  fi
  commit_and_push_if_changed "preview: remove PR #${PR_NUMBER}"
}

main() {
  parse_args "$@"
  if [[ -z "${CMD}" ]]; then
    usage
    exit 1
  fi

  require_env GITHUB_TOKEN
  require_env REPO
  trap cleanup EXIT
  init_branch_workdir

  case "${CMD}" in
    deploy-prod) deploy_prod ;;
    deploy-pr) deploy_pr ;;
    remove-pr) remove_pr ;;
    *)
      echo "ERROR: unknown command: ${CMD}" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
