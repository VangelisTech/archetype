#!/usr/bin/env bash
# Copyright 2025 Vangelis Technologies Inc.
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
# Review-thread disposition in one command. Encapsulates the standing ritual
# that every PR pays by hand today (~10 raw API calls per PR): reply to the
# thread with what changed, resolve it, and — after the LAST thread — submit
# a re-evaluation review so the Auto-merge workflow re-checks queue-readiness.
#
# GitHub Actions cannot observe thread resolution (the webhook is Apps-only),
# so resolving threads changes nothing until a pull_request_review event
# fires. Submitting a review is the only signal that re-evaluates arming
# WITHOUT re-reviewing the head; re-running the Deterministic Review Gate on
# a head with advisory findings republishes them as fresh unresolved threads.
#
# Usage:
#   scripts/gh_dispose_thread.sh <owner/repo> <pr> --list
#       List unresolved threads: id, path:line, outdated?, first-comment
#       excerpt. Use this to find thread ids.
#
#   scripts/gh_dispose_thread.sh <owner/repo> <pr> <thread-id> "<reply body>"
#       Reply to the thread and resolve it, atomically from the caller's
#       point of view (reply first, then resolve; if resolve fails the reply
#       stands and the command exits nonzero — rerun is safe, it adds a
#       second reply but resolution is idempotent).
#
#   scripts/gh_dispose_thread.sh <owner/repo> <pr> --signal
#       Submit the queue-readiness re-evaluation review for the current
#       exact head. Run ONCE, after the last thread is resolved.
#
# Exit codes: 0 ok; 2 usage; 3 API failure; 4 thread not found/already
# resolved (for the disposition form).
set -euo pipefail

if [ $# -lt 3 ]; then
  echo "usage: $0 <owner/repo> <pr> --list | --signal | <thread-id> \"<reply>\"" >&2
  exit 2
fi

REPO="$1"
PR="$2"
MODE="$3"
OWNER="${REPO%%/*}"
NAME="${REPO##*/}"

list_threads() {
  gh api graphql \
    -f query='
      query($owner: String!, $name: String!, $pr: Int!) {
        repository(owner: $owner, name: $name) {
          pullRequest(number: $pr) {
            reviewThreads(first: 100) {
              nodes {
                id isResolved isOutdated path line
                comments(first: 1) { nodes { body } }
              }
            }
          }
        }
      }' \
    -f owner="$OWNER" -f name="$NAME" -F pr="$PR" |
    jq -r '
      .data.repository.pullRequest.reviewThreads.nodes[]
      | select(.isResolved | not)
      | "\(.id)\t\(.path // "?"):\(.line // "?")\t\(if .isOutdated then "outdated" else "current" end)\t\(.comments.nodes[0].body // "" | gsub("\n"; " ") | .[0:120])"
    '
}

case "$MODE" in
  --list)
    out=$(list_threads)
    if [ -z "$out" ]; then
      echo "no unresolved threads on ${REPO}#${PR}."
    else
      printf '%s\n' "$out"
    fi
    ;;

  --signal)
    head_sha=$(gh pr view "$PR" --repo "$REPO" --json headRefOid --jq '.headRefOid')
    unresolved=$(list_threads | wc -l | tr -d ' ')
    if [ -n "$(list_threads)" ]; then
      echo "warning: ${unresolved} thread(s) still unresolved — the signal will not arm anything until they are resolved." >&2
    fi
    gh pr review "$PR" --repo "$REPO" --comment \
      --body "Queue-readiness re-evaluation on exact head ${head_sha}: review threads addressed and resolved."
    echo "re-evaluation review submitted for head ${head_sha}."
    ;;

  *)
    THREAD_ID="$MODE"
    BODY="${4:?usage: $0 <owner/repo> <pr> <thread-id> \"<reply>\"}"

    resolved=$(gh api graphql \
      -f query='
        query($id: ID!) {
          node(id: $id) { ... on PullRequestReviewThread { isResolved } }
        }' \
      -f id="$THREAD_ID" --jq '.data.node.isResolved' 2>/dev/null) || {
      echo "error: thread ${THREAD_ID} not found." >&2
      exit 4
    }
    if [ "$resolved" = "true" ]; then
      echo "thread ${THREAD_ID} is already resolved; nothing to do."
      exit 0
    fi

    gh api graphql \
      -f query='
        mutation($id: ID!, $body: String!) {
          addPullRequestReviewThreadReply(
            input: {pullRequestReviewThreadId: $id, body: $body}
          ) { comment { id } }
        }' \
      -f id="$THREAD_ID" -f body="$BODY" >/dev/null

    gh api graphql \
      -f query='
        mutation($id: ID!) {
          resolveReviewThread(input: {threadId: $id}) {
            thread { isResolved }
          }
        }' \
      -f id="$THREAD_ID" --jq '.data.resolveReviewThread.thread.isResolved' |
      grep -q true || {
        echo "error: reply posted but resolve failed for ${THREAD_ID}; rerun this command." >&2
        exit 3
      }
    echo "thread ${THREAD_ID}: replied and resolved."
    echo "reminder: after the LAST thread, run: $0 ${REPO} ${PR} --signal"
    ;;
esac
