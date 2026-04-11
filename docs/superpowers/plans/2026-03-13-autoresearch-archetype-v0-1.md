# AutoResearch-on-Archetype v0.1 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the minimal app/framework surface in `archetype` to model git-native experiments over a single tracked branch head, run them in bounded episodes, record results, and advance the frontier only on improvement.

**Architecture:** Keep `core/` unchanged. Add git-aware experiment primitives and state transitions in the app layer, plus a transactional git adapter and a reference `autoresearch` controller. Use existing async worlds for bounded execution, but keep git side effects isolated behind a single module.

**Tech Stack:** Python 3.12+, Pydantic models, existing `archetype.app` orchestration, existing async world/runtime, pytest

---

## Chunk 1: Model The Experiment Loop

### Task 1: Add git-native experiment models

**Files:**

- Modify: `src/archetype/app/models.py`
- Test: `tests/app/test_experiment_models.py`

- [ ] **Step 1: Write the failing test**

```python
from archetype.app.models import BranchHead, Experiment, RunRecord, ResultRecord


def test_experiment_tracks_repo_branch_and_base_commit():
    experiment = Experiment(
        repository="repo",
        branch_name="autoresearch/mar13",
        base_commit_hash="abc1234",
        proposal_summary="baseline tweak",
        world_spec={"world_name": "experiment-world"},
        run_spec={"steps": 1, "debug": False},
        evaluation_spec={"primary_metric_name": "val_bpb", "direction": "min"},
    )

    assert experiment.repository == "repo"
    assert experiment.branch_name == "autoresearch/mar13"
    assert experiment.base_commit_hash == "abc1234"
    assert experiment.world_spec["world_name"] == "experiment-world"
    assert experiment.run_spec["steps"] == 1
    assert experiment.evaluation_spec["primary_metric_name"] == "val_bpb"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/app/test_experiment_models.py -v`
Expected: FAIL because the new git-native model types do not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
class ExperimentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CRASHED = "crashed"
    KEPT = "kept"
    DISCARDED = "discarded"
```

Add first-class records for:

- `RepositoryRecord`
- `BranchHead`
- `CommitRecord`
- `Experiment`
- `RunRecord`
- `ResultRecord`

Keep them thin and explicit. Use Pydantic models that can be persisted later without redesign.

Make `Experiment` explicitly carry the runtime recipe:

- `world_spec`
- `run_spec`
- `evaluation_spec`

Treat these as declarative definitions. The experiment should reference how to instantiate a world and derive a concrete `RunConfig`, but it should not itself be a `World` or `RunConfig`.

Use a small canonical shape in the initial implementation:

- `world_spec`: world name, storage backend tuple, cache settings, metadata
- `run_spec`: budget kind/value plus the subset that maps directly onto `RunConfig`
- `evaluation_spec`: primary metric name, comparison direction, optional secondary metrics, crash policy

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/app/test_experiment_models.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/app/test_experiment_models.py src/archetype/app/models.py
git commit -m "feat: add git-native experiment models"
```

### Task 2: Encode branch-head advancement rules

**Files:**

- Create: `src/archetype/app/experiment_state.py`
- Test: `tests/app/test_experiment_state.py`

- [ ] **Step 1: Write the failing test**

```python
from archetype.app.experiment_state import advance_branch_head_if_improved
from archetype.app.models import BranchHead, Experiment, ResultRecord


def test_kept_result_advances_branch_head():
    branch = BranchHead(
        repository="repo",
        branch_name="autoresearch/mar13",
        current_commit_hash="abc1234",
        frontier_metric_name="val_bpb",
        frontier_metric_direction="min",
        frontier_metric_value=1.0,
    )
    experiment = Experiment(
        repository="repo",
        branch_name="autoresearch/mar13",
        base_commit_hash="abc1234",
        proposal_summary="lower bpb",
    )
    result = ResultRecord(
        experiment_id=experiment.experiment_id,
        primary_metric_name="val_bpb",
        primary_metric_value=0.99,
        primary_metric_direction="min",
    )

    updated = advance_branch_head_if_improved(branch, experiment, result, "def5678")

    assert updated.current_commit_hash == "def5678"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/app/test_experiment_state.py -v`
Expected: FAIL because the state-transition helpers do not exist.

- [ ] **Step 3: Write minimal implementation**

```python
def advance_branch_head_if_improved(branch, experiment, result, commit_hash):
    if result.primary_metric_value < branch.frontier_metric_value:
        branch.current_commit_hash = commit_hash
    return branch
```

Replace the placeholder with explicit, readable rules for:

- experiment/run state transitions
- keep/discard/crash classification
- branch-head advancement only on frontier improvement

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/app/test_experiment_state.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/app/test_experiment_state.py src/archetype/app/experiment_state.py src/archetype/app/models.py
git commit -m "feat: add experiment state transitions"
```

## Chunk 2: Add The Transactional Git Adapter

### Task 3: Add a single app-layer git transaction module

**Files:**

- Create: `src/archetype/app/git_runtime.py`
- Test: `tests/app/test_git_runtime.py`

- [ ] **Step 1: Write the failing test**

```python
from archetype.app.git_runtime import GitRuntimeRequest, GitRuntime


def test_git_runtime_request_is_explicit_about_branch_coordinates(tmp_path):
    request = GitRuntimeRequest(
        repository_path=str(tmp_path),
        branch_name="autoresearch/mar13",
        base_commit_hash="abc1234",
        proposal_summary="test change",
    )

    assert request.branch_name == "autoresearch/mar13"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/app/test_git_runtime.py -v`
Expected: FAIL because the git runtime adapter does not exist.

- [ ] **Step 3: Write minimal implementation**

```python
class GitRuntime:
    async def materialize(self, request: GitRuntimeRequest) -> GitRuntimeSession:
        ...
```

Design this module as the single transactional boundary for git side effects:

- resolve tracked branch head
- materialize checkout or worktree
- apply changes
- commit or rollback
- return resulting git coordinates and artifact paths

Do not mix these responsibilities into the orchestrator or model layer.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/app/test_git_runtime.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/app/test_git_runtime.py src/archetype/app/git_runtime.py
git commit -m "feat: add transactional git runtime adapter"
```

## Chunk 3: Add The Reference AutoResearch Controller

### Task 4: Add an experiment service that coordinates models, git, and runs

**Files:**

- Create: `src/archetype/app/experiment_service.py`
- Modify: `src/archetype/app/orchestrator.py`
- Test: `tests/app/test_experiment_service.py`

- [ ] **Step 1: Write the failing test**

```python
from archetype.app.experiment_service import ExperimentService


async def test_service_keeps_only_when_result_improves_frontier():
    service = ExperimentService(...)
    outcome = await service.run_experiment(...)
    assert outcome.decision in {"kept", "discarded", "crashed"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/app/test_experiment_service.py -v`
Expected: FAIL because the coordination service does not exist.

- [ ] **Step 3: Write minimal implementation**

```python
class ExperimentService:
    async def run_experiment(self, request):
        session = await self.git_runtime.materialize(request.git_request)
        run = self._start_run(...)
        result = await self._execute_run(...)
        return self._finalize(...)
```

Use this service to:

- create experiment/run records
- invoke the transactional git adapter
- execute a bounded run
- emit a result
- apply keep/discard/crash transition rules

Prefer small helper methods over a monolithic coordinator.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/app/test_experiment_service.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/app/test_experiment_service.py src/archetype/app/experiment_service.py src/archetype/app/orchestrator.py
git commit -m "feat: add experiment orchestration service"
```

### Task 5: Add a reference AutoResearch loop

**Files:**

- Create: `examples/autoresearch_loop.py`
- Test: `tests/integration/test_autoresearch_loop.py`

- [ ] **Step 1: Write the failing test**

```python
async def test_autoresearch_loop_uses_tracked_branch_head(tmp_path):
    # Arrange a tiny fake repo and a mocked run executor
    # Assert that only improved experiments advance the frontier
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_autoresearch_loop.py -v`
Expected: FAIL because the reference loop does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
async def run_forever(service, branch_head, proposer):
    while True:
        request = await proposer.next_experiment(branch_head)
        outcome = await service.run_experiment(request)
        if outcome.decision == "kept":
            branch_head = outcome.branch_head
```

Keep this as a reference controller, not a framework singleton. It should demonstrate the intended semantics without overfitting the core.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_autoresearch_loop.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_autoresearch_loop.py examples/autoresearch_loop.py
git commit -m "feat: add autoresearch reference controller"
```

## Chunk 4: Verify And Document

### Task 6: Verify the new surface end to end

**Files:**

- Modify: `README.md`
- Modify: `docs/guide/index.md`
- Test: `tests/app/`
- Test: `tests/integration/test_autoresearch_loop.py`

- [ ] **Step 1: Write the failing test**

```python
def test_readme_examples_match_public_api():
    # Smoke test imported symbols and example snippets
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/app/ tests/integration/test_autoresearch_loop.py -v`
Expected: At least one failure until the docs and public API are aligned.

- [ ] **Step 3: Write minimal implementation**

```python
# Update docs to describe:
# - tracked branch head
# - experiment/run/result state machine
# - app-layer transactional git adapter
# - reference autoresearch controller
```

Document the boundary clearly:

- `core/` remains untouched
- `app/` owns git-native experiment orchestration
- the reference controller is a concrete implementation of the framework, not the framework itself

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/app/ tests/integration/test_autoresearch_loop.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add README.md docs/guide/index.md tests/app tests/integration/test_autoresearch_loop.py
git commit -m "docs: add git-native experiment orchestration guide"
```
