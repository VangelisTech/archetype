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

# Decide whether a PR is ready for auto-merge arming / may keep an existing arm.
#
# Queue-ready means:
#   1. latest review-complete check on the exact head concluded success, and
#   2. every non-outdated review thread is resolved.
#
# Why (2): Main — Review requires thread resolution. Arming on review-complete
# alone leaves auto-merge armed for hours while conversations stay open
# (PR #645). The "Revert premature arming" job then reports success because it
# classifies the arm as legitimate, which shepherds misread as "disarmed".
#
# Usage:
#   gh_pr_queue_ready.sh <owner/repo> <pr-number> <head-sha>
#
# Exit codes:
#   0 — queue-ready (stdout: "queue-ready")
#   1 — not queue-ready (stdout: one-line reason; never exits 0 on doubt)
#   2 — usage / tool failure

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <owner/repo> <pr-number> <head-sha>" >&2
  exit 2
fi

REPO="$1"
PR_NUMBER="$2"
HEAD_SHA="$3"

if [[ ! "$REPO" =~ ^[^/]+/[^/]+$ ]]; then
  echo "invalid repository: ${REPO}" >&2
  exit 2
fi
if [[ ! "$PR_NUMBER" =~ ^[0-9]+$ ]]; then
  echo "invalid PR number: ${PR_NUMBER}" >&2
  exit 2
fi
if [[ ! "$HEAD_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "invalid head sha: ${HEAD_SHA}" >&2
  exit 2
fi

OWNER="${REPO%%/*}"
NAME="${REPO##*/}"

# LATEST run only: a head's review can be re-triggered without a new commit,
# and a stale success must not outvote a newer failure. API failure falls
# through as not-ready (fail closed).
latest="$(
  gh api "repos/${REPO}/commits/${HEAD_SHA}/check-runs?check_name=review-complete&per_page=100" \
    --jq '[.check_runs[]] | sort_by(.started_at) | last | .conclusion // empty' \
    2>/dev/null || true
)"
if [[ "$latest" != "success" ]]; then
  echo "no authoritative passing review-complete on ${HEAD_SHA} (latest: ${latest:-none})"
  exit 1
fi

# Count non-outdated unresolved threads. Outdated threads on superseded lines
# must not block arming after the author pushed a fix that moved the hunk.
#
# hasNextPage is not decoration. Truncation is not an error: past 100 threads
# GitHub returns a clean, valid response containing exactly 100 nodes, so the
# count below is a perfectly good integer computed from an incomplete set and
# the guards cannot fire — nothing failed. If every unresolved thread sat past
# node 100 this would print "queue-ready" and exit 0 while the PR was actually
# blocked: a fail-open inside the one mechanism whose whole purpose is to fail
# closed. So ask whether there is more, and refuse when there is.
#
# Deliberately NOT a cursor loop. Paging adds a retry surface, more API calls,
# and a partial-failure mode midway through that would itself need a
# fail-closed decision. A PR carrying more than 100 review threads is
# pathological and deserves a human look, so refusing to arm it is the correct
# outcome and is honest about what was not checked. Five lines that refuse to
# guess beat forty that might — do not "improve" this into pagination.
threads="$(
  gh api graphql \
    -f query='query($owner:String!, $name:String!, $number:Int!) {
      repository(owner:$owner, name:$name) {
        pullRequest(number:$number) {
          reviewThreads(first:100) {
            pageInfo { hasNextPage }
            nodes { isResolved isOutdated }
          }
        }
      }
    }' \
    -f owner="$OWNER" \
    -f name="$NAME" \
    -F number="$PR_NUMBER" \
    --jq '.data.repository.pullRequest.reviewThreads
          | "\(.pageInfo.hasNextPage) \([.nodes[]
              | select(.isResolved == false and .isOutdated == false)] | length)"' \
    2>/dev/null || true
)"

truncated="${threads%% *}"
unresolved="${threads##* }"

if [[ -z "$unresolved" || ! "$unresolved" =~ ^[0-9]+$ ]]; then
  echo "could not enumerate review threads on #${PR_NUMBER}; refusing to treat as queue-ready"
  exit 1
fi

# Anything other than an explicit "false" — "true", "null", a shape change —
# means the page may be incomplete. Fail closed on all of them.
if [[ "$truncated" != "false" ]]; then
  echo "more than 100 review threads on #${PR_NUMBER}; refusing to treat as queue-ready without seeing them all"
  exit 1
fi

if [[ "$unresolved" -gt 0 ]]; then
  echo "review-complete passed on ${HEAD_SHA} but ${unresolved} unresolved review thread(s) remain"
  exit 1
fi

echo "queue-ready"
exit 0
