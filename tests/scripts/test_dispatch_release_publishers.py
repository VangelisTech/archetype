# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for exact-run release publisher dispatch and completion."""

from __future__ import annotations

import io
import json
from email.message import Message
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import unquote

import pytest

from scripts import dispatch_release_publishers as target

PARENT_RUN_ID = 123456
PARENT_RUN_ATTEMPT = 2
TAG = "v0.6.0"
COMMIT = "a" * 40
TAG_OBJECT = "b" * 40
TOKEN = "github-test-token"
REGISTRY = "testpypi"


class _Response:
    def __init__(
        self,
        url: str,
        payload: object,
        *,
        status: int = 200,
        final_url: str | None = None,
    ) -> None:
        self.status = status
        self._url = final_url or url
        self._body = json.dumps(payload).encode()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def geturl(self) -> str:
        return self._url

    def read(self, amount: int = -1) -> bytes:
        return self._body[:amount] if amount >= 0 else self._body


def _context() -> dict[str, Any]:
    return {
        "parent_run_id": PARENT_RUN_ID,
        "parent_run_attempt": PARENT_RUN_ATTEMPT,
        "tag": TAG,
        "expected_commit": COMMIT,
        "expected_tag_object": TAG_OBJECT,
        "registry": REGISTRY,
    }


def _run_id(index: int) -> int:
    return 9000 + index


def _allowlist() -> dict[str, Any]:
    return {
        "schema": target.SCHEMA,
        "repository": target.REPOSITORY,
        "parent_run_id": PARENT_RUN_ID,
        "parent_run_attempt": PARENT_RUN_ATTEMPT,
        "tag": TAG,
        "commit": COMMIT,
        "tag_object": TAG_OBJECT,
        "registry": REGISTRY,
        "runs": [
            {
                "distribution": distribution,
                "workflow": workflow,
                "run_id": _run_id(index),
                "url": target._run_html_url(_run_id(index)),
            }
            for index, (distribution, workflow) in enumerate(target._publishers(), start=1)
        ],
    }


def _headers(request: Any) -> dict[str, str]:
    return {key.lower(): value for key, value in request.header_items()}


def _run_payload(
    run: dict[str, Any],
    *,
    status: str = "completed",
    conclusion: str | None = "success",
) -> dict[str, Any]:
    run_id = run["run_id"]
    return {
        "id": run_id,
        "path": f".github/workflows/{run['workflow']}",
        "event": "workflow_dispatch",
        "head_sha": COMMIT,
        "head_branch": TAG,
        "repository": {"full_name": target.REPOSITORY},
        "status": status,
        "conclusion": conclusion,
        "url": target._run_api_url(run_id),
        "html_url": target._run_html_url(run_id),
    }


def test_dispatch_posts_every_child_and_returns_exact_allowlist() -> None:
    publishers = target._publishers()
    calls: list[tuple[str, dict[str, Any], float]] = []

    def open_request(request: Any, *, timeout: float) -> _Response:
        workflow = unquote(request.full_url.split("/workflows/", 1)[1].split("/", 1)[0])
        index = [value for _, value in publishers].index(workflow) + 1
        payload = json.loads(request.data)
        calls.append((workflow, payload, timeout))
        headers = _headers(request)
        assert request.get_method() == "POST"
        assert headers["accept"] == "application/vnd.github+json"
        assert headers["authorization"] == f"Bearer {TOKEN}"
        assert headers["x-github-api-version"] == target.API_VERSION
        run_id = _run_id(index)
        return _Response(
            request.full_url,
            {
                "workflow_run_id": run_id,
                "run_url": target._run_api_url(run_id),
                "html_url": target._run_html_url(run_id),
            },
        )

    receipt = target.dispatch_publishers(
        **_context(),
        token=TOKEN,
        open_request=open_request,
        http_timeout_seconds=17,
    )

    assert receipt == _allowlist()
    assert [workflow for workflow, _payload, _timeout in calls] == [
        workflow for _distribution, workflow in publishers
    ]
    assert all(timeout == 17 for _workflow, _payload, timeout in calls)
    assert [payload for _workflow, payload, _timeout in calls] == [
        {
            "ref": TAG,
            "return_run_details": True,
            "inputs": {
                "parent_run_id": str(PARENT_RUN_ID),
                "parent_run_attempt": str(PARENT_RUN_ATTEMPT),
                "tag": TAG,
                "expected_commit": COMMIT,
                "expected_tag_object": TAG_OBJECT,
                "registry": REGISTRY,
            },
        }
        for _ in publishers
    ]
    assert all(workflow != target.RELEASE_WORKFLOW for _distribution, workflow in publishers)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("workflow_run_id", 0),
        ("run_url", "https://api.github.com/repos/other/repo/actions/runs/9001"),
        ("html_url", "https://github.com/other/repo/actions/runs/9001"),
    ],
)
def test_dispatch_rejects_inexact_run_details(field: str, value: object) -> None:
    def open_request(request: Any, *, timeout: float) -> _Response:
        del timeout
        payload: dict[str, object] = {
            "workflow_run_id": 9001,
            "run_url": target._run_api_url(9001),
            "html_url": target._run_html_url(9001),
        }
        payload[field] = value
        return _Response(request.full_url, payload)

    with pytest.raises(target.PublisherDispatchError, match="exact run ID|run URLs"):
        target.dispatch_publishers(
            **_context(),
            token=TOKEN,
            open_request=open_request,
        )


def test_await_polls_only_allowlisted_ids_until_every_run_succeeds() -> None:
    allowlist = _allowlist()
    polls: dict[int, int] = {}
    sleeps: list[float] = []
    now = [0.0]

    def open_request(request: Any, *, timeout: float) -> _Response:
        assert request.get_method() == "GET"
        assert timeout <= 11
        run_id = int(request.full_url.rsplit("/", 1)[1])
        assert run_id in {run["run_id"] for run in allowlist["runs"]}
        polls[run_id] = polls.get(run_id, 0) + 1
        run = next(run for run in allowlist["runs"] if run["run_id"] == run_id)
        if polls[run_id] == 1:
            payload = _run_payload(run, status="queued", conclusion=None)
        else:
            payload = _run_payload(run)
        return _Response(request.full_url, payload)

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    receipt = target.await_publishers(
        allowlist,
        **_context(),
        token=TOKEN,
        timeout_seconds=30,
        interval_seconds=3,
        http_timeout_seconds=11,
        open_request=open_request,
        sleep=sleep,
        monotonic=lambda: now[0],
    )

    assert receipt == allowlist
    assert polls == {run["run_id"]: 2 for run in allowlist["runs"]}
    assert sleeps == [3]


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/{workflow}",
        ".github/workflows/{workflow}@{tag}",
        ".github/workflows/{workflow}@refs/tags/{tag}",
    ],
)
def test_await_accepts_only_exact_supported_workflow_path_forms(path: str) -> None:
    allowlist = _allowlist()
    runs = {run["run_id"]: run for run in allowlist["runs"]}

    def open_request(request: Any, *, timeout: float) -> _Response:
        del timeout
        run_id = int(request.full_url.rsplit("/", 1)[1])
        run = runs[run_id]
        payload = _run_payload(run)
        payload["path"] = path.format(workflow=run["workflow"], tag=TAG)
        return _Response(request.full_url, payload)

    assert (
        target.await_publishers(
            allowlist,
            **_context(),
            token=TOKEN,
            open_request=open_request,
        )
        == allowlist
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", 999999),
        ("path", ".github/workflows/release.yml"),
        ("event", "push"),
        ("head_sha", "b" * 40),
        ("head_branch", "v0.6.3"),
        ("url", "https://api.github.com/repos/other/repo/actions/runs/9001"),
        ("html_url", "https://github.com/other/repo/actions/runs/9001"),
        ("repository", {"full_name": "other/repo"}),
    ],
)
def test_await_rejects_wrong_run_identity(field: str, value: object) -> None:
    allowlist = _allowlist()
    first = allowlist["runs"][0]

    def open_request(request: Any, *, timeout: float) -> _Response:
        del timeout
        payload = _run_payload(first)
        payload[field] = value
        return _Response(request.full_url, payload)

    with pytest.raises(target.PublisherDispatchError, match="unexpected"):
        target.await_publishers(
            allowlist,
            **_context(),
            token=TOKEN,
            open_request=open_request,
        )


def test_await_fails_on_a_completed_non_successful_run() -> None:
    allowlist = _allowlist()
    first = allowlist["runs"][0]

    def open_request(request: Any, *, timeout: float) -> _Response:
        del timeout
        return _Response(request.full_url, _run_payload(first, conclusion="failure"))

    with pytest.raises(target.PublisherDispatchError, match="completed with 'failure'"):
        target.await_publishers(
            allowlist,
            **_context(),
            token=TOKEN,
            open_request=open_request,
        )


def test_await_timeout_and_interval_are_bounded() -> None:
    allowlist = _allowlist()
    runs = {run["run_id"]: run for run in allowlist["runs"]}
    calls: list[int] = []
    sleeps: list[float] = []
    now = [0.0]

    def open_request(request: Any, *, timeout: float) -> _Response:
        del timeout
        run_id = int(request.full_url.rsplit("/", 1)[1])
        calls.append(run_id)
        return _Response(
            request.full_url,
            _run_payload(runs[run_id], status="in_progress", conclusion=None),
        )

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    with pytest.raises(target.PublisherDispatchError, match="within 5 seconds"):
        target.await_publishers(
            allowlist,
            **_context(),
            token=TOKEN,
            timeout_seconds=5,
            interval_seconds=2,
            open_request=open_request,
            sleep=sleep,
            monotonic=lambda: now[0],
        )

    assert calls == [run["run_id"] for run in allowlist["runs"]] * 3
    assert sleeps == [2, 2, 1]
    with pytest.raises(ValueError, match="at most 3600"):
        target.await_publishers(
            allowlist,
            **_context(),
            token=TOKEN,
            timeout_seconds=target.MAX_TIMEOUT_SECONDS + 1,
        )
    with pytest.raises(ValueError, match="at most 60"):
        target.await_publishers(
            allowlist,
            **_context(),
            token=TOKEN,
            interval_seconds=target.MAX_INTERVAL_SECONDS + 1,
        )


def test_allowlist_rejects_wrong_context_order_and_forged_urls() -> None:
    wrong_context = _allowlist()
    wrong_context["parent_run_attempt"] = PARENT_RUN_ATTEMPT + 1
    with pytest.raises(target.PublisherDispatchError, match="parent run attempt"):
        target.validate_allowlist(wrong_context, **_context())

    wrong_tag_object = _allowlist()
    wrong_tag_object["tag_object"] = "c" * 40
    with pytest.raises(target.PublisherDispatchError, match="tag object"):
        target.validate_allowlist(wrong_tag_object, **_context())

    wrong_order = _allowlist()
    wrong_order["runs"][0], wrong_order["runs"][1] = (
        wrong_order["runs"][1],
        wrong_order["runs"][0],
    )
    with pytest.raises(target.PublisherDispatchError, match="distribution order"):
        target.validate_allowlist(wrong_order, **_context())

    forged = _allowlist()
    forged["runs"][0]["url"] = "https://github.com/other/repo/actions/runs/9001"
    with pytest.raises(target.PublisherDispatchError, match="wrong URL"):
        target.validate_allowlist(forged, **_context())


@pytest.mark.parametrize("tag_object", ["", "A" * 40, "a" * 39, 7, None])
def test_context_rejects_malformed_tag_object(tag_object: object) -> None:
    context = _context()
    context["expected_tag_object"] = tag_object

    with pytest.raises(ValueError, match="expected tag object"):
        target.validate_allowlist(_allowlist(), **context)


def test_allowlist_rejects_the_previous_schema() -> None:
    allowlist = _allowlist()
    allowlist["schema"] = "archetype.publisher-dispatch/v1"

    with pytest.raises(target.PublisherDispatchError, match="unexpected schema"):
        target.validate_allowlist(allowlist, **_context())


def test_github_api_errors_are_diagnostic_and_redirects_fail_closed() -> None:
    def forbidden(request: Any, *, timeout: float) -> _Response:
        del timeout
        raise HTTPError(
            request.full_url,
            403,
            "Forbidden",
            hdrs=Message(),
            fp=io.BytesIO(b'{"message":"permission denied"}'),
        )

    with pytest.raises(target.GitHubAPIError, match="HTTP 403: permission denied"):
        target.dispatch_publishers(
            **_context(),
            token=TOKEN,
            open_request=forbidden,
        )

    def unavailable(request: Any, *, timeout: float) -> _Response:
        del request, timeout
        raise URLError("offline")

    with pytest.raises(target.GitHubAPIError, match="offline"):
        target.dispatch_publishers(
            **_context(),
            token=TOKEN,
            open_request=unavailable,
        )

    def redirected(request: Any, *, timeout: float) -> _Response:
        del timeout
        return _Response(
            request.full_url,
            {},
            final_url="https://example.invalid/redirected",
        )

    with pytest.raises(target.GitHubAPIError, match="redirected"):
        target.dispatch_publishers(
            **_context(),
            token=TOKEN,
            open_request=redirected,
        )


@pytest.mark.parametrize("token", ["", " leading", "two words", "line\nbreak", "snowman-☃"])
def test_invalid_tokens_are_rejected_before_network(token: str) -> None:
    called = False

    def open_request(_request: Any, *, timeout: float) -> _Response:
        nonlocal called
        del timeout
        called = True
        raise AssertionError("network must not be called")

    with pytest.raises(ValueError, match="GitHub token"):
        target.dispatch_publishers(
            **_context(),
            token=token,
            open_request=open_request,
        )
    assert called is False


def test_receipt_serialization_is_deterministic(tmp_path: Any) -> None:
    receipt = _allowlist()
    path = tmp_path / "publisher-dispatch.json"

    target._write_json(path, receipt)

    assert (
        path.read_text(encoding="utf-8")
        == json.dumps(
            receipt,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
