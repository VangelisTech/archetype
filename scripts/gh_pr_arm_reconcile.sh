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

# Reconcile one PR's auto-merge arm with its actual queue-readiness.
#
# The automerge workflow is event-driven, and GitHub Actions delivers no
# event for the state change that most often completes queue-readiness:
# resolving the last review thread (`pull_request_review_thread` is an Apps
# webhook, not an Actions trigger). This script is the event-free half of the
# arming contract: given a PR, read what is true NOW and make the arm match.
#
#   eligible + queue-ready + unarmed          -> arm (squash), verify
#   armed + no longer queue-ready             -> disarm, verify
#   already held by the merge queue           -> no-op (the merge-group
#                                                recheck owns that phase)
#   ineligible (closed, draft, wrong author)  -> no-op
#   state already correct                     -> no-op
#
# Queue-readiness is decided ONLY by scripts/gh_pr_queue_ready.sh — the same
# oracle the automerge arm/guard/dequeue jobs run. This script adds no second
# definition of ready; it only acts on the answer.
#
# MIRROR NOTE: the eligibility filter (open, non-draft, author) restates the
# filter inside automerge.yml's arm job. tests/scripts/test_queue_reevaluator.py
# holds the two literals together; change them together or not at all.
#
# Usage:
#   gh_pr_arm_reconcile.sh <owner/repo> <pr-number>
#
# Exit codes:
#   0 — reconciled or nothing to do (stdout says which)
#   1 — arm/disarm postcondition failed after retries
#   2 — usage / tool failure
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <owner/repo> <pr-number>" >&2
  exit 2
fi

REPO="$1"
PR="$2"
OWNER="${REPO%%/*}"
NAME="${REPO##*/}"

# Mirrors automerge.yml's arm-job filter (see MIRROR NOTE above).
MAINTAINER="everettVT"

if [[ ! "$PR" =~ ^[0-9]+$ ]]; then
  echo "invalid PR number: ${PR}" >&2
  exit 2
fi

meta="$(
  gh api graphql \
    -f query='query($owner:String!, $name:String!, $number:Int!) {
      repository(owner:$owner, name:$name) {
        pullRequest(number:$number) {
          state
          isDraft
          author { login }
          headRefOid
          autoMergeRequest { enabledAt }
          mergeQueueEntry { position }
        }
      }
    }' \
    -f owner="$OWNER" -f name="$NAME" -F number="$PR" \
    --jq '.data.repository.pullRequest
          | "\(.state) \(.isDraft) \(.author.login) \(.headRefOid) \(.autoMergeRequest != null) \(.mergeQueueEntry != null)"' \
    2>/dev/null || true
)"
if [[ -z "$meta" ]]; then
  echo "could not read PR #${PR}; leaving arm state untouched" >&2
  exit 2
fi
read -r state draft author head_sha armed queued <<<"$meta"

if [[ "$state" != "OPEN" || "$draft" != "false" || "$author" != "$MAINTAINER" ]]; then
  echo "PR #${PR} is not eligible (state=${state} draft=${draft} author=${author}); nothing to reconcile."
  exit 0
fi

if [[ "$queued" == "true" ]]; then
  echo "PR #${PR} is already held by the merge queue; the merge-group recheck owns this phase."
  exit 0
fi

set +e
reason=$(scripts/gh_pr_queue_ready.sh "$REPO" "$PR" "$head_sha")
ready=$?
set -e
if [[ "$ready" -ne 0 && "$ready" -ne 1 ]]; then
  echo "queue-ready helper failed (exit ${ready}): ${reason}" >&2
  exit 2
fi

# Postcondition over exit code, in both directions: `gh pr merge` exits
# nonzero on benign no-ops, and a transient API failure must not be mistaken
# for success. Read the arm state back and require it to match.
reconcile() {
  local want="$1"
  shift
  local attempt observed
  for attempt in 1 2 3; do
    gh pr merge "$@" --repo "$REPO" "$PR" 2>/dev/null || true
    observed=$(gh pr view "$PR" --repo "$REPO" \
      --json autoMergeRequest --jq '.autoMergeRequest != null')
    if [[ "$observed" == "$want" ]]; then
      return 0
    fi
    sleep 5
  done
  return 1
}

if [[ "$ready" -eq 0 && "$armed" != "true" ]]; then
  if reconcile true --auto --squash; then
    echo "PR #${PR} is queue-ready on ${head_sha}; armed auto-merge."
    exit 0
  fi
  echo "could not arm queue-ready PR #${PR} after 3 attempts" >&2
  exit 1
fi

if [[ "$ready" -eq 1 && "$armed" == "true" ]]; then
  if reconcile false --disable-auto; then
    echo "PR #${PR} is no longer queue-ready (${reason}); disarmed auto-merge."
    exit 0
  fi
  echo "could not disarm PR #${PR} (${reason}); a stale arm may enter the queue" >&2
  exit 1
fi

if [[ "$ready" -eq 0 ]]; then
  echo "PR #${PR} is queue-ready and already armed; nothing to do."
else
  echo "PR #${PR} is not queue-ready (${reason}) and not armed; nothing to do."
fi
exit 0
