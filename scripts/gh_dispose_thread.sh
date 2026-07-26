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
# Fail-closed contract:
# - --signal refuses while any unresolved thread remains: the review it
#   submits is the arming re-evaluation signal, and posting it over open
#   threads would assert "addressed and resolved" falsely.
# - A thread page past 100 entries is refused, not silently truncated —
#   the emptiness of the unresolved list must never be an artifact of
#   pagination (same stance as scripts/gh_pr_queue_ready.sh; a PR carrying
#   100+ threads deserves a human look, not a cursor loop).
# - Disposition validates that the thread belongs to the requested repo and
#   PR before mutating anything: thread ids are global node ids, and a
#   pasted id from another PR must not be replied to and resolved while
#   this command reports success for the requested target.
#
# Usage:
#   scripts/gh_dispose_thread.sh <owner/repo> <pr> --list
#       List unresolved threads: id, path:line, outdated?, first-comment
#       excerpt. Use this to find thread ids.
#
#   scripts/gh_dispose_thread.sh <owner/repo> <pr> <thread-id> "<reply body>"
#       Reply to the thread and resolve it (reply first, then resolve; if
#       resolve fails the reply stands and the command exits nonzero —
#       rerun is safe, it adds a second reply but resolution is idempotent).
#
#   scripts/gh_dispose_thread.sh <owner/repo> <pr> --signal
#       Submit the queue-readiness re-evaluation review for the current
#       exact head. Refuses while unresolved threads remain.
#
# Exit codes: 0 ok; 1 refused (unresolved threads / truncation); 2 usage;
# 3 API failure; 4 thread not found, already resolved, or owned by a
# different PR.
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

# One fetch serves every mode: raw JSON, so truncation and unresolved-ness
# are read from the same snapshot instead of separate queries racing each
# other.
fetch_threads_json() {
  gh api graphql \
    -f query='
      query($owner: String!, $name: String!, $pr: Int!) {
        repository(owner: $owner, name: $name) {
          pullRequest(number: $pr) {
            reviewThreads(first: 100) {
              pageInfo { hasNextPage }
              nodes {
                id isResolved isOutdated path line
                comments(first: 1) { nodes { body } }
              }
            }
          }
        }
      }' \
    -f owner="$OWNER" -f name="$NAME" -F pr="$PR"
}

threads_truncated() {
  jq -r '.data.repository.pullRequest.reviewThreads.pageInfo.hasNextPage' <<<"$1"
}

unresolved_tsv() {
  jq -r '
    .data.repository.pullRequest.reviewThreads.nodes[]
    | select(.isResolved | not)
    | "\(.id)\t\(.path // "?"):\(.line // "?")\t\(if .isOutdated then "outdated" else "current" end)\t\(.comments.nodes[0].body // "" | gsub("\n"; " ") | .[0:120])"
  ' <<<"$1"
}

refuse_truncation() {
  echo "error: ${REPO}#${PR} has more than 100 review threads; refusing to" \
       "act on a truncated page (the unresolved list could be an artifact" \
       "of pagination). This PR needs a human look." >&2
  exit 1
}

case "$MODE" in
  --list)
    json=$(fetch_threads_json)
    [ "$(threads_truncated "$json")" = "false" ] || refuse_truncation
    out=$(unresolved_tsv "$json")
    if [ -z "$out" ]; then
      echo "no unresolved threads on ${REPO}#${PR}."
    else
      printf '%s\n' "$out"
    fi
    ;;

  --signal)
    json=$(fetch_threads_json)
    [ "$(threads_truncated "$json")" = "false" ] || refuse_truncation
    open=$(unresolved_tsv "$json")
    if [ -n "$open" ]; then
      count=$(printf '%s\n' "$open" | wc -l | tr -d ' ')
      echo "error: ${count} unresolved thread(s) remain on ${REPO}#${PR};" \
           "refusing to submit the re-evaluation review. Dispose them first:" >&2
      printf '%s\n' "$open" >&2
      exit 1
    fi
    head_sha=$(gh pr view "$PR" --repo "$REPO" --json headRefOid --jq '.headRefOid')
    gh pr review "$PR" --repo "$REPO" --comment \
      --body "Queue-readiness re-evaluation on exact head ${head_sha}: review threads addressed and resolved."
    echo "re-evaluation review submitted for head ${head_sha}."
    ;;

  *)
    THREAD_ID="$MODE"
    BODY="${4:?usage: $0 <owner/repo> <pr> <thread-id> \"<reply>\"}"

    # The id is a global node id; bind it to the requested repo/PR before
    # mutating anything (see fail-closed contract above).
    owner_info=$(gh api graphql \
      -f query='
        query($id: ID!) {
          node(id: $id) {
            ... on PullRequestReviewThread {
              isResolved
              pullRequest { number repository { nameWithOwner } }
            }
          }
        }' \
      -f id="$THREAD_ID" \
      --jq '.data.node | "\(.isResolved) \(.pullRequest.number) \(.pullRequest.repository.nameWithOwner)"' \
      2>/dev/null) || {
      echo "error: thread ${THREAD_ID} not found." >&2
      exit 4
    }
    read -r resolved thread_pr thread_repo <<<"$owner_info"
    if [ "$thread_repo" != "$REPO" ] || [ "$thread_pr" != "$PR" ]; then
      echo "error: thread ${THREAD_ID} belongs to ${thread_repo}#${thread_pr}," \
           "not ${REPO}#${PR}; refusing to touch it." >&2
      exit 4
    fi
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

    # Suggest --signal only when it would actually be accepted.
    json=$(fetch_threads_json)
    if [ "$(threads_truncated "$json")" != "false" ]; then
      echo "note: over 100 threads on this PR; remaining count unknown."
    else
      remaining=$(unresolved_tsv "$json")
      if [ -z "$remaining" ]; then
        echo "all threads resolved. Now run: $0 ${REPO} ${PR} --signal"
      else
        count=$(printf '%s\n' "$remaining" | wc -l | tr -d ' ')
        echo "${count} unresolved thread(s) remain; dispose them before --signal."
      fi
    fi
    ;;
esac
