# Trajectory Sampling & Labeling — Design Spec

*Everett Kleven, March 2026*

> Canonicalize the way we work with AI session data across all interaction sources. Define sampling regimes, labeling strategies, and the DSL for trajectory analysis.

---

## Problem

We have years of AI interaction data across multiple platforms (Claude Code, ChatGPT, custom agents). The synth engine trains a contrastive encoder on labeled text segments — but "labeled text segments" is a lossy abstraction. A conversation is a dynamical system: the user's intent evolves, the assistant's state changes, decisions compound, and the outcome (a git diff, a deployed feature, a discarded approach) is the integral of the whole trajectory.

We need a framework that treats interaction data at the right level of resolution for the question being asked, not a one-size-fits-all "extract user messages and label them."

---

## Sampling Regimes

A trajectory is a time-ordered sequence of turns. Different questions require sampling at different resolutions — analogous to choosing your sample rate relative to the signal bandwidth.

### Regime 1: Message-level (Nyquist on utterances)

**Unit**: A single message (user or assistant).

**What it captures**: Linguistic style, vocabulary, framing preferences, question patterns, instruction specificity.

**Sampling**:
- User messages: what the person *asks for* and *how they ask*
- Assistant messages: what was *produced* and *how it was framed*
- Filter: strip system prompts, XML scaffolding, tool call JSON. Keep the human-readable content.

**Label axes**:
| Label | Source | Cost |
|-------|--------|------|
| `user_id` | Metadata | Free |
| `project` | Metadata | Free |
| `role` (user/assistant) | Metadata | Free |
| `perspective` (objective/subjective/abjective/superjective) | Mind extraction | API call |
| `voice` (actor/observer/observed) | Mind extraction | API call |
| `memory_type` (user/project/feedback/reference) | Mind extraction | API call |
| `intent` (instruct/question/feedback/correction/approval) | New classifier | API call |

**Contrastive pairs**:
- Positive: same user, different project (style persists across context)
- Negative: different user, same project (project doesn't determine style)
- Hard negative: same project, similar topic, different user

**Use when**: Building a user fingerprint. "Who wrote this?" "What does this person care about?"

### Regime 2: Turn-level (Nyquist on interactions)

**Unit**: A (user message, assistant response) pair. The atomic unit of interaction.

**What it captures**: The relationship between request and response. How the user reacts to different approaches. What kinds of assistant behavior get approved vs corrected.

**Sampling**:
- Pair adjacent user→assistant messages by timestamp within a session
- Include the user's *next* message as implicit feedback on the assistant's response
- Triplet: (user_msg, assistant_response, user_reaction)

**Label axes**:
| Label | Source | Cost |
|-------|--------|------|
| `outcome` (accepted/corrected/rejected/ignored) | Heuristic on next user message | Free |
| `agency` (user-proposed/assistant-proposed/collaborative) | Classifier | API call |
| `decision_type` (commit/defer/reverse/explore) | Decision Analysis (#40) | API call |
| `tool_usage` (read/edit/bash/search/none) | Tool call metadata | Free |
| `complexity` (single-step/multi-step/research/debugging) | Heuristic on tool count | Free |

**Contrastive pairs**:
- Positive: same user, same outcome (accepted turns share something)
- Negative: same user, opposite outcome (accepted vs corrected)
- Cross-user: different users, same outcome pattern

**Use when**: Understanding decision dynamics. "What makes this user accept a suggestion?" "When do they correct?"

### Regime 3: Episode-level (Nyquist on tasks)

**Unit**: A contiguous sequence of turns toward a single goal within a session. Bounded by topic shifts, `/clear` commands, or long time gaps.

**What it captures**: Task structure, problem-solving patterns, escalation behavior, persistence vs pivoting.

**Sampling**:
- Segment sessions into episodes by:
  - Time gaps > 5 minutes
  - `/clear` or `/compact` commands
  - Project directory changes
  - Explicit topic shifts ("actually, let's do X instead")
- Summarize each episode: initial request, approach taken, outcome, length

**Label axes**:
| Label | Source | Cost |
|-------|--------|------|
| `task_type` (bugfix/feature/refactor/research/config/review) | Classifier on first user msg | API call |
| `outcome` (completed/abandoned/deferred/escalated) | Heuristic on last messages | Free |
| `length` (turns, wall clock) | Metadata | Free |
| `correction_count` | Count of user corrections in episode | Free |
| `tool_diversity` | Unique tool types used | Free |
| `approach` (direct/iterative/exploratory/systematic) | Classifier | API call |

**Contrastive pairs**:
- Positive: same user, same task type, similar outcome
- Negative: same task type, different user, different approach
- Temporal: same user, same task type, early vs late sessions (learning/drift)

**Use when**: Understanding work patterns. "How does this person debug?" "Do they plan before implementing?"

### Regime 4: Trajectory-level (Nyquist on projects)

**Unit**: An entire session or multi-session arc tied to a project outcome (a merged PR, a deployed feature, an abandoned branch).

**What it captures**: The full arc from intent to outcome. Strategic choices. The relationship between conversation patterns and code quality.

**Sampling**:
- Link sessions to git history via timestamps and project paths
- A trajectory = all sessions within a project branch's lifetime
- Ground truth: the git diff, PR review comments, CI pass/fail

**Label axes**:
| Label | Source | Cost |
|-------|--------|------|
| `pr_outcome` (merged/closed/abandoned) | Git/GitHub API | Free |
| `review_feedback` (approved/changes_requested/commented) | GitHub API | Free |
| `lines_changed` | Git diff | Free |
| `test_delta` (tests added/modified/broken) | Git diff | Free |
| `time_to_merge` | Git timestamps | Free |
| `session_count` | Count sessions in trajectory | Free |
| `strategic_coherence` | Classifier: did the approach stay consistent? | API call |

**Contrastive pairs**:
- Positive: same user, successful trajectories (merged, clean reviews)
- Negative: same user, failed trajectories (abandoned, heavily corrected)
- Cross-user: different users solving similar problems

**Use when**: Evaluating end-to-end effectiveness. "Which interaction patterns lead to good outcomes?"

---

## DSL for Trajectory Analysis

The sampling regimes form a hierarchy. The DSL should let you move between levels fluently.

```
Trajectory (project arc)
  └── Episode[] (task segments)
       └── Turn[] (user ↔ assistant pairs)
            └── Message[] (individual utterances)
```

### Core abstractions

```python
@dataclass
class Message:
    text: str
    role: str              # "user" | "assistant"
    user_id: str
    timestamp: int
    session_id: str
    project: str
    tools_used: list[str]  # tool names from assistant messages

@dataclass
class Turn:
    user_msg: Message
    assistant_msg: Message
    user_reaction: Message | None  # next user message (implicit feedback)
    outcome: str           # "accepted" | "corrected" | "rejected" | "ignored"

@dataclass
class Episode:
    turns: list[Turn]
    task_type: str
    outcome: str
    duration_s: float
    correction_count: int

@dataclass
class Trajectory:
    episodes: list[Episode]
    project: str
    user_id: str
    git_outcome: dict      # PR state, lines changed, review feedback
```

### Sampling API

```python
# Message-level: flat DataFrame of all messages
messages = sample_messages(conversations, regime="message")

# Turn-level: paired user-assistant with reaction
turns = sample_turns(conversations, regime="turn")

# Episode-level: segmented by task boundaries
episodes = sample_episodes(conversations, regime="episode", gap_threshold_s=300)

# Trajectory-level: linked to git outcomes
trajectories = sample_trajectories(conversations, git_repo, regime="trajectory")
```

### Labeler interface

```python
class Labeler(Protocol):
    """A labeler adds one or more label columns to a DataFrame."""

    label_name: str
    regime: str  # which regime this labeler operates on
    cost: str    # "free" | "api_call" | "gpu"

    def label(self, df: daft.DataFrame) -> daft.DataFrame:
        """Add label column(s) to the DataFrame."""
        ...
```

**Free labelers** (metadata-derived):
- `UserIdLabeler` — from directory structure
- `ProjectLabeler` — from session metadata
- `RoleLabeler` — user vs assistant
- `ToolUsageLabeler` — parse tool calls from assistant messages
- `TurnOutcomeLabeler` — heuristic on user's next message
- `EpisodeLengthLabeler` — turn count, wall clock

**API labelers** (LLM-powered):
- `PerspectiveLabeler` — objective/subjective/abjective/superjective
- `VoiceLabeler` — actor/observer/observed
- `IntentLabeler` — instruct/question/feedback/correction/approval
- `DecisionLabeler` — commit/defer/reverse/explore
- `AgencyLabeler` — who proposed, who accepted
- `TaskTypeLabeler` — bugfix/feature/refactor/research

**Derived labelers** (post-training):
- `ClusterLabeler` — from encoder clustering (the recursion loop)
- `DriftLabeler` — temporal shift detection
- `AnomalyLabeler` — outlier scoring

---

## Labeler Composition

Labelers compose. Run free labelers first, train an initial encoder, then use the encoder's clusters to decide which API labelers to run (and on which subset of data).

```
Phase 0: Free labels (user_id, project, role, tool_usage, turn_outcome)
          → generate_triplets() → train encoder v0

Phase 1: Cluster with v0 → identify under-separated groups
          → run API labelers on boundary segments only (10% of data, not 100%)
          → retrain encoder v1

Phase 2: Cluster with v1 → stable structure
          → link to git outcomes (trajectory-level)
          → train encoder v2 with trajectory-aware triplets
```

This is the budget-optimal path: spend API calls only where the free labels can't resolve the structure.

---

## Data Sources (Current)

| Source | Format | Messages | Users | Status |
|--------|--------|----------|-------|--------|
| claude-shared | JSONL (history + sessions) | ~4,947 (history), ~50k+ (sessions) | 13 | Loader written |
| mind_extraction.json | JSON | 35 | 1 | Working |
| Git history (archetype) | Commits, diffs | ~200 commits | 5+ | Not yet wired |
| ChatGPT exports | JSON | TBD | 1 | Data exists, no loader |
| Custom agent logs | Varies | TBD | TBD | Future |

---

## Implementation Order

1. **Message-level with free labels** — `user_id` + `project` + `role` as label axes on the claude-shared history data. No API calls. Train encoder, measure if it can distinguish users. This is the immediate next step.

2. **Turn-level pairing** — parse session JSONLs into (user, assistant, reaction) triples. Add `outcome` heuristic. Train with turn-level contrastive signal.

3. **Episode segmentation** — split sessions by time gaps and topic shifts. Add episode-level labels.

4. **API labelers on boundary segments** — run mind extraction / decision analysis only on segments the encoder can't cleanly cluster.

5. **Trajectory linking** — connect sessions to git outcomes. This is the endgame: the encoder learns which interaction patterns produce good code.

---

## Open Questions

- **User deduplication**: `everett` and `everettvt` are the same person. How to handle? Merge at load time, or keep separate and let the encoder discover it?
- **Assistant model variation**: Sessions span different Claude models. Does model version matter for user behavior analysis?
- **Privacy**: claude-shared is a team repo. Labeling someone's cognitive patterns from their work conversations has implications. Keep the encoder weights private, publish the framework.
- **Temporal weighting**: Recent sessions should matter more. Exponential decay? Sliding window? Or let the encoder learn temporal structure from the timestamp feature?

---

## Revision: Canonical Event Schema (v2)

*Added March 2026 after exhaustive audit of claude-shared data and architectural critique.*

### The Fundamental Correction

v1 of this spec treated conversations as a band-limited sequence of utterances. They are not. A conversation is a **branched, event-driven, hybrid closed-loop system**. The canonical layer must be an **append-only event graph**, and the four sampling regimes must be **materialized views** over it — not the primary storage.

The loader was stripping tool calls, progress events, compaction boundaries, subagent forks, and model metadata. This is the equivalent of low-passing a control signal before identifying the plant. We preserve everything at ingest; views filter at query time.

### Event Types (discovered in claude-shared)

| Type | Fields | What it captures |
|------|--------|-----------------|
| `user` | uuid, parentUuid, message.content, toolUseResult (stdout/stderr), sourceToolAssistantUUID, cwd, gitBranch, sessionId, timestamp, isMeta, isSidechain, thinkingMetadata, permissionMode | User messages, tool approval results, local commands |
| `assistant` | uuid, parentUuid, message.content (text + tool_use + thinking blocks), message.model, requestId, cwd, gitBranch, sessionId, timestamp, isSidechain | Responses, tool invocations, model version, thinking traces |
| `progress` | uuid, toolUseID, parentToolUseID, data (bash_progress / hook_progress with output, elapsed time), cwd, gitBranch, timestamp | Streaming tool execution — the actual work being done |
| `system` | uuid, subtype (turn_duration / compact_boundary / microcompact_boundary / stop_hook_summary / api_error / local_command), durationMs, cwd, gitBranch, timestamp | Lifecycle events: timing, compaction, errors, hooks |
| `summary` | summary, leafUuid | What the model thought mattered when context was compressed |
| `file-history-snapshot` | messageId, snapshot (trackedFileBackups with versions v1/v2/v3), isSnapshotUpdate | File state at conversation points — edit progression |
| `queue-operation` | operation (enqueue/dequeue/remove), content, sessionId, timestamp | Async background task management |

### Canonical Event Record

```python
@dataclass
class Event:
    """The atom of the event graph. Everything else is a view."""

    # Identity
    uuid: str
    parent_uuid: str | None           # DAG edge to parent event
    event_type: str                    # user | assistant | progress | system | summary | file_snapshot | queue

    # Content (type-dependent)
    text: str | None                   # Human-readable content (extracted from message parts)
    tool_calls: list[ToolCall] | None  # From assistant content blocks
    tool_result: ToolResult | None     # From user toolUseResult
    progress_data: dict | None         # From progress events (bash output, elapsed time)
    system_subtype: str | None         # compact_boundary, turn_duration, api_error, etc.
    summary_text: str | None           # From summary events
    file_snapshot: dict | None         # From file-history-snapshot events

    # Context (present on all events)
    user_id: str
    session_id: str
    timestamp: int
    cwd: str | None
    git_branch: str | None
    is_sidechain: bool                 # True for subagent conversations
    model: str | None                  # Assistant model version (e.g., "claude-opus-4-5-20251101")

    # DAG metadata
    tool_use_id: str | None            # Links progress events to their tool call
    parent_tool_use_id: str | None     # Links tool calls to their parent
    source_tool_assistant_uuid: str | None  # Links tool results back to the assistant turn
    request_id: str | None             # Claude API request ID (groups streaming chunks)

@dataclass
class ToolCall:
    id: str
    name: str                          # Bash, Read, Edit, Write, Grep, Glob, Agent, etc.
    input: dict                        # Tool input parameters

@dataclass
class ToolResult:
    stdout: str
    stderr: str
    interrupted: bool
    is_image: bool
```

### DAG Topology

The event graph is a **forest of shallow DAGs**:

```
Session (root)
├── user msg (uuid=A)
│   └── assistant response (parentUuid=A, uuid=B)
│       ├── tool_use: Bash (uuid=C)
│       │   ├── progress: bash_progress (parentToolUseID=C)
│       │   ├── progress: bash_progress (parentToolUseID=C)
│       │   └── user: toolUseResult (sourceToolAssistantUUID=B)
│       └── tool_use: Agent (uuid=D)
│           └── [subagent session file: agent-{id}.jsonl]
│               ├── user msg (isSidechain=true)
│               ├── assistant response (isSidechain=true)
│               └── ... (own DAG subtree)
├── system: compact_boundary          ← context compression point
├── summary: "what was happening"     ← model's compression of prior context
├── user msg (uuid=E, parentUuid=B)   ← conversation continues
│   └── ...
```

**Branch points**: subagent spawns, `/clear` commands, worktree switches
**Implicit merges**: subagent results return via parentUuid threading (no explicit merge node)
**Compaction boundaries**: system events with subtype `compact_boundary` / `microcompact_boundary` mark where the model lost detailed context

### Views Over the Event Graph

The four sampling regimes from v1 become **projection operators** on this graph:

```python
def project_messages(events: EventGraph) -> DataFrame:
    """Regime 1: Filter to user/assistant events, extract text content."""
    return events.where(type in ("user", "assistant")).select(text, user_id, ...)

def project_turns(events: EventGraph) -> DataFrame:
    """Regime 2: Pair user→assistant by parentUuid, attach reaction."""
    # Follow parentUuid edges to pair user messages with their responses
    # Attach the NEXT user event as implicit reaction
    ...

def project_episodes(events: EventGraph, gap_s: int = 300) -> DataFrame:
    """Regime 3: Segment by compact_boundary events and time gaps."""
    # Use system:compact_boundary as hard episode boundaries
    # Use time gaps > gap_s as soft boundaries
    ...

def project_trajectories(events: EventGraph, git_repo: str) -> DataFrame:
    """Regime 4: Group sessions by project+branch, link to git outcomes."""
    # Group events by (project, gitBranch)
    # Join to git log for commit/PR/merge data
    ...
```

### Outcome as State Estimator

v1 used "next user message" as a heuristic for outcome. This conflates new reference inputs with error signals. The revised approach treats outcome as a **multi-signal state estimate**:

```python
@dataclass
class OutcomeEstimate:
    """Inferred from multiple observation channels, not a single heuristic."""

    # Local observations (from conversation)
    explicit_approval: float    # P(approved) from keyword detection
    explicit_rejection: float   # P(rejected) from correction patterns
    directive_change: float     # P(pivot) from "actually", "let's try X instead"
    engagement_velocity: float  # Messages per minute (high = engaged)
    interruption: bool          # [Request interrupted by user] marker

    # Tool-mediated observations
    commit_created: bool        # Git commit after this turn
    tests_passed: bool | None   # Test results if available
    files_reverted: bool        # File version went backwards
    pr_created: bool            # PR submission gate-pass

    # Temporal observations
    response_latency_s: float   # Time until user's next message
    session_continued: bool     # Did the user keep going?
    re_engaged_later: bool      # Did they come back to this project?

    # Derived
    confidence: float           # How confident are we in the estimate
    outcome: str                # "accepted" | "corrected" | "rejected" | "abandoned" | "deferred" | "unclear"
```

Signals ranked by reliability (from agent audit):
1. **VERY HIGH**: Explicit success markers, architectural rejections, test failures, commit/PR gates
2. **HIGH**: Directive changes, plan approvals, negation patterns, file version progression, recurring errors
3. **MEDIUM**: Single-word affirmations, timestamp gaps, option selection, async delegation
4. **LOW**: Empty messages, truncations, silence without context

### Active Labeling: Mixture Sampling (revised)

v1 proposed boundary-only API labeling. This has a positive-feedback failure mode: if initial free-label geometry is biased toward metadata, boundary sampling refines those same basins.

Revised sampling mixture:
- **Cluster boundary uncertainty** (30%) — segments the encoder can't cleanly assign
- **Regime disagreement** (20%) — message-level label differs from turn-level or episode-level label
- **Temporal novelty / drift** (15%) — segments from time periods underrepresented in training
- **Branch bifurcation points** (15%) — moments where the user changed direction (subagent spawns, pivots, corrections)
- **External outcome importance** (20%) — segments from trajectories with strong outcomes (merged cleanly, abandoned after heavy effort, tests broke then recovered)

---

## Data Sources: Ingestion Pipelines & Schemas

### Source 1: Claude Code (JSONL)

**Location**: `claude-shared/users/{user_id}/`
**Format**: JSONL — one JSON object per line
**Two tiers**:
- `history.jsonl` — top-level user prompts (`display`, `project`, `sessionId`, `timestamp`)
- `projects/{path}/{sessionId}.jsonl` — full event streams (all 7 event types)
- `projects/{path}/{sessionId}/subagents/agent-{id}.jsonl` — forked subagent conversations

**Ingestion pipeline**:
1. Walk `users/` directory tree
2. For each `.jsonl` file, parse events preserving ALL fields
3. Resolve `parentUuid` edges within session, `sourceToolAssistantUUID` cross-links
4. Link subagent files to parent sessions via directory structure
5. Emit canonical `Event` records

**Schema mapping**:
| Claude Code field | Canonical field |
|-------------------|----------------|
| `type` | `event_type` |
| `uuid` | `uuid` |
| `parentUuid` | `parent_uuid` |
| `message.content[].text` | `text` |
| `message.content[].type=tool_use` | `tool_calls` |
| `toolUseResult` | `tool_result` |
| `data` (progress events) | `progress_data` |
| `subtype` (system events) | `system_subtype` |
| `message.model` | `model` |
| `cwd` | `cwd` |
| `gitBranch` | `git_branch` |
| `isSidechain` | `is_sidechain` |
| `sessionId` | `session_id` |
| Directory `users/{name}` | `user_id` |

**Volume**: 13 users, 2,625 session files, 553 subagent files, ~50k+ events

### Source 2: ChatGPT Exports (JSON)

**Format**: Single JSON file per export with `conversations[]` array
**Structure**:
```json
{
  "conversations": [
    {
      "title": "...",
      "create_time": 1234567890,
      "mapping": {
        "node_id": {
          "id": "...",
          "parent": "parent_node_id",
          "message": {
            "author": {"role": "user|assistant|system"},
            "content": {"content_type": "text", "parts": ["..."]},
            "create_time": 1234567890
          },
          "children": ["child_node_id_1", ...]
        }
      }
    }
  ]
}
```

**Key differences from Claude Code**:
- Already a DAG: `mapping` is an explicit node graph with `parent` and `children`
- No tool call metadata (pre-GPT-4 turbo) or limited (post)
- No `cwd`, `gitBranch`, or project context — conversations are free-floating
- No streaming progress events — only final messages
- Model version in metadata, not per-message

**Schema mapping**:
| ChatGPT field | Canonical field |
|---------------|----------------|
| `mapping[id].message.author.role` | `event_type` (user/assistant) |
| `mapping[id].id` | `uuid` |
| `mapping[id].parent` | `parent_uuid` |
| `mapping[id].message.content.parts[0]` | `text` |
| `conversation.create_time` | `timestamp` |
| `conversation.title` | (used for episode labeling) |
| Export filename / account | `user_id` |
| N/A | `cwd`, `git_branch`, `tool_calls` — not available |

**Volume**: Varies by user. Typically 100s-1000s of conversations over years.

### Source 3: Git History (Commits as Interaction Artifacts)

**Format**: `git log` output, diffs, PR metadata via `gh` CLI
**Structure**:
```
commit {sha}
Author: {name} <{email}>
Date:   {timestamp}

    {message}

diff --git a/{file} b/{file}
...
```

**Key difference**: Git history is the *outcome* of trajectories, not the conversation. It provides ground truth for trajectory-level labels.

**Schema mapping**:
| Git field | Canonical field |
|-----------|----------------|
| `commit.sha` | `uuid` |
| `commit.parent` | `parent_uuid` (merge commits have 2+) |
| `commit.message` | `text` |
| `commit.author` | `user_id` |
| `commit.timestamp` | `timestamp` |
| `diff` | `tool_result` (the artifact produced) |
| Branch name | `git_branch` |
| PR state (merged/closed) | Used for trajectory outcome labels |

**Volume**: Hundreds of commits per repo, with full diffs.

### Source 4: Custom Agent Logs (Future)

**Format**: Varies — Langchain traces (JSON), AutoGPT logs, custom frameworks
**Key challenge**: No standard schema. Each framework logs different events at different granularity.
**Approach**: Define adapter interface per source, map to canonical `Event` records.

### Canonical Schema Alignment

All sources converge to the same `Event` record. Fields that don't exist in a source are `None`:

| Field | Claude Code | ChatGPT | Git | Custom |
|-------|------------|---------|-----|--------|
| `uuid` | `uuid` | `mapping[id].id` | `commit.sha` | Framework-specific |
| `parent_uuid` | `parentUuid` | `mapping[id].parent` | `commit.parent` | Framework-specific |
| `text` | `message.content` | `parts[0]` | `commit.message` | Varies |
| `tool_calls` | From content blocks | Limited/None | N/A | Varies |
| `tool_result` | `toolUseResult` | N/A | `diff` | Varies |
| `cwd` | `cwd` | N/A | Repo root | Varies |
| `git_branch` | `gitBranch` | N/A | Branch name | N/A |
| `model` | `message.model` | Metadata | N/A | Varies |
| `is_sidechain` | `isSidechain` | N/A | N/A | Varies |
| `user_id` | Directory name | Account | `commit.author` | Varies |

---

## Dual Encoder Architecture (revised)

v1 used a single mean-pooled 256-token encoder for everything. This erases order, phase, reversals, and branch structure — exactly the nonlinear dynamics that matter for trajectory analysis.

Revised architecture:

### Path 1: Semantic Encoder (existing)
The `BidirectionalEncoder` (2M params, 128-dim) operates on individual messages. It captures *what was said*, not *how the conversation evolved*. Keep it for message-level and turn-level regimes.

### Path 2: Dynamical Features (new)
For episode and trajectory regimes, compute explicit control-theoretic summary statistics from the event graph:

| Feature | What it measures | Analogous to |
|---------|-----------------|-------------|
| `settling_time` | Turns from task start to stable output | Transient response |
| `overshoot` | Max correction magnitude before settling | Peak overshoot |
| `correction_gain` | How much the user's correction changes the output | Open-loop gain |
| `oscillation_score` | Repeated back-and-forth on the same decision | Limit cycle detection |
| `branch_fanout` | Number of subagents/worktrees spawned | State space dimension |
| `rollback_rate` | Fraction of changes reverted | Tracking error integral |
| `tool_effort_integral` | Total tool calls weighted by complexity | Control effort ∫u²dt |
| `engagement_bandwidth` | Message frequency spectrum (fast=broadband, slow=narrowband) | Signal bandwidth |
| `compaction_density` | Compact boundaries per unit time | Information loss rate |
| `dwell_time` | Time spent before first correction | Phase margin |

### Path 3: Temporal/Graph Aggregator (future)
A sequence model or GNN that operates on the event graph directly. Takes semantic embeddings + dynamical features as node attributes, learns trajectory-level representations. This is the endgame model — not built yet.

```
Event Graph
  → Semantic Encoder (per-node text embeddings)
  → Dynamical Feature Extraction (per-episode/trajectory stats)
  → Graph/Sequence Aggregator (trajectory embedding)
  → Contrastive Loss (same user similar trajectories close, different users far)
```
