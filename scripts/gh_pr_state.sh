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
# One-line PR state: everything an agent polls for, in a single GraphQL
# round-trip. Replaces the hand-rolled `gh pr view` / `gh pr checks` /
# raw-GraphQL polling zoo (gh's --json has no mergeQueueEntry field, and
# mergeStateStatus reads UNKNOWN during queue finalization — neither is a
# reason for a dozen separate probes).
#
# Usage:
#   scripts/gh_pr_state.sh <owner/repo> <pr-number>            # one line, exit 0
#   scripts/gh_pr_state.sh <owner/repo> <pr-number> --watch    # poll every 30s,
#                                                              # print only on
#                                                              # state CHANGE
#   scripts/gh_pr_state.sh <owner/repo> <pr-number> --watch 15 # custom interval
#
# Output line format (stable, greppable):
#   pr=683 head=59ab3ec7 state=OPEN draft=false merge=BLOCKED \
#   review-complete=FAILURE threads=2/7-unresolved(1-outdated) armed=false \
#   queue=none
#
# --watch terminates on its own when the PR reaches MERGED or CLOSED, and
# after MAX_POLLS regardless (no orphaned loops; macOS has no `timeout`).
# Exit codes: 0 normal; 2 usage; 3 API failure.
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <owner/repo> <pr-number> [--watch [interval-seconds]]" >&2
  exit 2
fi

REPO="$1"
PR="$2"
WATCH=false
INTERVAL=30
MAX_POLLS=240 # 2h at the default interval — a bound, not a target.
if [ "${3:-}" = "--watch" ]; then
  WATCH=true
  INTERVAL="${4:-30}"
fi

OWNER="${REPO%%/*}"
NAME="${REPO##*/}"

QUERY='
query($owner: String!, $name: String!, $pr: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $pr) {
      state
      isDraft
      mergeStateStatus
      headRefOid
      autoMergeRequest { enabledAt }
      mergeQueueEntry { position estimatedTimeToMerge state }
      reviewThreads(first: 100) {
        pageInfo { hasNextPage }
        nodes { isResolved isOutdated }
      }
      commits(last: 1) {
        nodes {
          commit {
            statusCheckRollup {
              contexts(first: 100) {
                nodes {
                  ... on CheckRun { name conclusion status startedAt }
                }
              }
            }
          }
        }
      }
    }
  }
}'

snapshot() {
  local raw
  if ! raw=$(gh api graphql \
    -f query="$QUERY" \
    -f owner="$OWNER" -f name="$NAME" -F pr="$PR" 2>&1); then
    echo "error: GraphQL query failed: ${raw}" >&2
    return 3
  fi
  jq -r --arg pr "$PR" '
    .data.repository.pullRequest as $p
    | ($p.reviewThreads.nodes // []) as $threads
    | ($threads | map(select(.isResolved | not))) as $open
    | ($open | map(select(.isOutdated))) as $outdated
    # Newest run wins: review-complete can be re-run on an unchanged head,
    # and connection order is unspecified — sort by start time like the
    # queue-ready oracle does, so a stale success never outvotes a newer
    # failure.
    | ([$p.commits.nodes[0].commit.statusCheckRollup.contexts.nodes[]?
        | select(.name == "review-complete")]
       | sort_by(.startedAt) | last) as $rc
    | "pr=\($pr) head=\($p.headRefOid[0:8])"
      + " state=\($p.state)"
      + " draft=\($p.isDraft)"
      + " merge=\($p.mergeStateStatus)"
      + " review-complete=\(if $rc == null then "absent"
          elif $rc.conclusion != null then $rc.conclusion
          else $rc.status end)"
      # Past 100 threads the counts are computed from an incomplete page;
      # say so instead of printing confident wrong numbers.
      + " threads=\(if $p.reviewThreads.pageInfo.hasNextPage
          then "TRUNCATED(>100)"
          else "\($open | length)/\($threads | length)-unresolved(\($outdated | length)-outdated)"
          end)"
      + " armed=\($p.autoMergeRequest != null)"
      + " queue=\(if $p.mergeQueueEntry == null then "none"
          else "pos-\($p.mergeQueueEntry.position)"
            + "/\($p.mergeQueueEntry.state)"
            + (if $p.mergeQueueEntry.estimatedTimeToMerge != null
               then "/eta-\($p.mergeQueueEntry.estimatedTimeToMerge)s"
               else "" end)
          end)"
  ' <<<"$raw"
}

if ! $WATCH; then
  snapshot
  exit $?
fi

prev=""
polls=0
while [ "$polls" -lt "$MAX_POLLS" ]; do
  line=$(snapshot) || exit 3
  if [ "$line" != "$prev" ]; then
    printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$line"
    prev="$line"
  fi
  case "$line" in
    *state=MERGED*|*state=CLOSED*) exit 0 ;;
  esac
  polls=$((polls + 1))
  sleep "$INTERVAL"
done
echo "watch: reached ${MAX_POLLS} polls without terminal state; stopping." >&2
