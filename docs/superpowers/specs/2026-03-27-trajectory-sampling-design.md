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
