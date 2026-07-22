# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Missions read-projection route contracts."""

import pytest
from fastapi.testclient import TestClient

from archetype.api.app import create_app
from archetype.api.deps import set_container
from archetype.app.container import ServiceContainer
from archetype.missions.components import (
    AgentExecution,
    Mission,
    MissionState,
    Task,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    ValidationResult,
)
from archetype.missions.relations import DependsOn, Guards, PartOfMission


@pytest.fixture
def client():
    container = ServiceContainer()
    set_container(container)
    app = create_app()
    with TestClient(app) as c:
        yield c
    set_container(None)


def _spawn(client, world_id, components):
    resp = client.post(
        f"/worlds/{world_id}/entities",
        json={"components": [c.to_payload() for c in components]},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["entity_id"]


@pytest.fixture
def mission_world(client, tmp_path):
    resp = client.post(
        "/worlds",
        json={"name": "missions_api", "storage_uri": str(tmp_path / "store")},
    )
    assert resp.status_code == 201, resp.text
    world_id = resp.json()["world_id"]

    mission_id = _spawn(client, world_id, [Mission(name="m1"), MissionState()])
    task_a = _spawn(
        client,
        world_id,
        [Task(name="pin-commit"), TaskState(), TaskDispatch(), TaskPolicy()],
    )
    task_b = _spawn(
        client,
        world_id,
        [
            Task(name="normalize-toolchain"),
            TaskState(),
            TaskDispatch(dispatch_id="d-1", sequence=1),
            TaskPolicy(),
        ],
    )
    validator = _spawn(
        client,
        world_id,
        [TaskValidator(name="tests-pass", command=["make", "test"])],
    )
    execution = _spawn(
        client,
        world_id,
        [
            AgentExecution(
                task_id=task_b,
                dispatch_id="d-1",
                dispatch_sequence=1,
                sandbox_id="sb-1",
                agent_session_id="sess-1",
            )
        ],
    )
    _spawn(
        client,
        world_id,
        [
            ValidationResult(
                task_id=task_b,
                validator_id=validator,
                execution_id=execution,
                dispatch_id="d-1",
                dispatch_sequence=1,
                revision="abc123",
                actual_returncode=0,
            )
        ],
    )
    _spawn(client, world_id, [PartOfMission(source=task_a, target=mission_id)])
    _spawn(client, world_id, [PartOfMission(source=task_b, target=mission_id)])
    _spawn(client, world_id, [DependsOn(source=task_b, target=task_a)])
    _spawn(client, world_id, [Guards(source=validator, target=task_b)])

    step = client.post(f"/worlds/{world_id}/step", json={})
    assert step.status_code == 200, step.text
    return {
        "world_id": world_id,
        "mission_id": mission_id,
        "task_a": task_a,
        "task_b": task_b,
        "validator": validator,
    }


class TestMissionRoutes:
    def test_list_missions(self, client, mission_world):
        resp = client.get(f"/worlds/{mission_world['world_id']}/missions")
        assert resp.status_code == 200, resp.text
        rows = resp.json()
        assert len(rows) == 1
        row = rows[0]
        assert row["entity_id"] == mission_world["mission_id"]
        assert row["mission__name"] == "m1"
        assert row["missionstate__status"] == "running"

    def test_mission_task_dag(self, client, mission_world):
        world_id = mission_world["world_id"]
        resp = client.get(f"/worlds/{world_id}/missions/{mission_world['mission_id']}/tasks")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        task_ids = {row["entity_id"] for row in body["tasks"]}
        assert task_ids == {mission_world["task_a"], mission_world["task_b"]}
        assert body["depends_on"] == [
            {"source": mission_world["task_b"], "target": mission_world["task_a"]}
        ]

    def test_mission_without_tasks_is_empty(self, client, mission_world):
        world_id = mission_world["world_id"]
        resp = client.get(f"/worlds/{world_id}/missions/999999/tasks")
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"mission_id": 999999, "tasks": [], "depends_on": []}

    def test_task_card(self, client, mission_world):
        world_id = mission_world["world_id"]
        task_b = mission_world["task_b"]
        resp = client.get(f"/worlds/{world_id}/tasks/{task_b}")
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert [row["entity_id"] for row in body["task"]] == [task_b]
        assert body["task"][0]["taskdispatch__sequence"] == 1

        assert [row["entity_id"] for row in body["validators"]] == [mission_world["validator"]]
        assert body["validators"][0]["taskvalidator__name"] == "tests-pass"

        assert len(body["executions"]) == 1
        assert body["executions"][0]["agentexecution__agent_session_id"] == "sess-1"

        assert len(body["validations"]) == 1
        validation = body["validations"][0]
        assert validation["validationresult__revision"] == "abc123"
        assert validation["validationresult__actual_returncode"] == 0

    def test_task_card_not_found(self, client, mission_world):
        world_id = mission_world["world_id"]
        resp = client.get(f"/worlds/{world_id}/tasks/999999")
        assert resp.status_code == 404

    def test_sparse_world_reads_empty_not_404(self, client, tmp_path):
        # A mission whose world has never spawned DependsOn/Guards/
        # AgentExecution/ValidationResult tables is a normal early state.
        resp = client.post(
            "/worlds",
            json={"name": "sparse", "storage_uri": str(tmp_path / "sparse")},
        )
        world_id = resp.json()["world_id"]
        mission = _spawn(client, world_id, [Mission(name="m"), MissionState()])
        task = _spawn(client, world_id, [Task(name="t"), TaskState(), TaskDispatch(), TaskPolicy()])
        _spawn(client, world_id, [PartOfMission(source=task, target=mission)])
        assert client.post(f"/worlds/{world_id}/step", json={}).status_code == 200

        dag = client.get(f"/worlds/{world_id}/missions/{mission}/tasks")
        assert dag.status_code == 200, dag.text
        assert [row["entity_id"] for row in dag.json()["tasks"]] == [task]
        assert dag.json()["depends_on"] == []

        card = client.get(f"/worlds/{world_id}/tasks/{task}")
        assert card.status_code == 200, card.text
        body = card.json()
        assert body["validators"] == []
        assert body["executions"] == []
        assert body["validations"] == []

    def test_unknown_world_reads_empty(self, client):
        # Query routes tolerate unknown world ids so persisted history stays
        # readable after a world instance is destroyed (see query.py _query_ids).
        resp = client.get("/worlds/00000000-0000-0000-0000-000000000000/missions")
        assert resp.status_code == 200
        assert resp.json() == []
