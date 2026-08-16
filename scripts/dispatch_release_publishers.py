#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dispatch and await the exact per-distribution release publisher runs."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import time
from collections.abc import Callable, Mapping, Sequence
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener

if __package__:
    from .release_artifact import DISTRIBUTIONS, PUBLISHER_WORKFLOWS
else:  # pragma: no cover - exercised by the command-line entry point
    from release_artifact import DISTRIBUTIONS, PUBLISHER_WORKFLOWS

API_VERSION = "2026-03-10"
API_ROOT = "https://api.github.com"
REPOSITORY = "VangelisTech/archetype"
SCHEMA = "archetype.publisher-dispatch/v2"
RELEASE_WORKFLOW = "release.yml"

DEFAULT_TIMEOUT_SECONDS = 30 * 60.0
DEFAULT_INTERVAL_SECONDS = 10.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0
MAX_TIMEOUT_SECONDS = 60 * 60.0
MAX_INTERVAL_SECONDS = 60.0
MAX_HTTP_TIMEOUT_SECONDS = 60.0
_MAX_RESPONSE_BYTES = 1024 * 1024
_TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_WORKFLOW = re.compile(r"[a-z0-9][a-z0-9-]*\.yml\Z")
_REGISTRIES = frozenset(("testpypi", "pypi"))
_IN_PROGRESS_STATUSES = frozenset(("queued", "in_progress", "requested", "waiting", "pending"))

OpenRequest = Callable[..., Any]
Sleep = Callable[[float], None]
Monotonic = Callable[[], float]


class PublisherDispatchError(RuntimeError):
    """The publisher run set could not be dispatched or verified safely."""


class GitHubAPIError(PublisherDispatchError):
    """GitHub returned an invalid response or could not be reached."""


class _RejectRedirects(HTTPRedirectHandler):
    """Do not forward the bearer token through an HTTP redirect."""

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        del req, fp, code, msg, headers, newurl
        return None


_OPEN = build_opener(_RejectRedirects()).open


def _positive_integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _duration(value: object, label: str, maximum: float) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
        or value > maximum
    ):
        raise ValueError(f"{label} must be greater than zero and at most {maximum:g} seconds")
    return float(value)


def _token(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 4096
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise ValueError("GitHub token must be non-empty printable ASCII without whitespace")
    return value


def _context(
    *,
    parent_run_id: object,
    parent_run_attempt: object,
    tag: object,
    expected_commit: object,
    expected_tag_object: object,
    registry: object,
) -> tuple[int, int, str, str, str, str]:
    run_id = _positive_integer(parent_run_id, "parent run ID")
    run_attempt = _positive_integer(parent_run_attempt, "parent run attempt")
    if not isinstance(tag, str) or _TAG.fullmatch(tag) is None:
        raise ValueError("release tag must be canonical vMAJOR.MINOR.PATCH")
    if not isinstance(expected_commit, str) or _COMMIT.fullmatch(expected_commit) is None:
        raise ValueError("expected commit must be one lowercase 40-hex SHA")
    if not isinstance(expected_tag_object, str) or _COMMIT.fullmatch(expected_tag_object) is None:
        raise ValueError("expected tag object must be one lowercase 40-hex SHA")
    if not isinstance(registry, str) or registry not in _REGISTRIES:
        raise ValueError("registry must be testpypi or pypi")
    return run_id, run_attempt, tag, expected_commit, expected_tag_object, registry


def _publishers() -> tuple[tuple[str, str], ...]:
    if tuple(PUBLISHER_WORKFLOWS) != DISTRIBUTIONS:
        raise PublisherDispatchError("publisher workflow mapping must match distribution order")
    publishers = tuple(
        (distribution, PUBLISHER_WORKFLOWS[distribution])
        for distribution in DISTRIBUTIONS
        if PUBLISHER_WORKFLOWS[distribution] != RELEASE_WORKFLOW
    )
    workflows = [workflow for _, workflow in publishers]
    if not publishers or len(set(workflows)) != len(workflows):
        raise PublisherDispatchError("publisher workflows must be non-empty and unique")
    if any(
        not isinstance(workflow, str) or _WORKFLOW.fullmatch(workflow) is None
        for workflow in workflows
    ):
        raise PublisherDispatchError("publisher workflow names must be safe .yml filenames")
    return publishers


def _response_detail(body: bytes) -> str:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        value = None
    if isinstance(value, dict) and isinstance(value.get("message"), str):
        return value["message"][:500]
    return body.decode("utf-8", errors="replace").strip()[:500] or "empty response"


def _read_response(response: Any) -> bytes:
    body = response.read(_MAX_RESPONSE_BYTES + 1)
    if not isinstance(body, bytes):
        raise GitHubAPIError("GitHub API response body must be bytes")
    if len(body) > _MAX_RESPONSE_BYTES:
        raise GitHubAPIError("GitHub API response exceeded the one-megabyte limit")
    return body


def _request_json(
    method: str,
    endpoint: str,
    *,
    token: str,
    payload: Mapping[str, Any] | None = None,
    open_request: OpenRequest = _OPEN,
    timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Call one fixed-repository GitHub endpoint and require a JSON object."""

    token = _token(token)
    timeout_seconds = _duration(
        timeout_seconds,
        "HTTP timeout",
        MAX_HTTP_TIMEOUT_SECONDS,
    )
    if method not in {"GET", "POST"} or not endpoint.startswith(f"/repos/{REPOSITORY}/actions/"):
        raise ValueError("GitHub API endpoint is outside the release Actions boundary")
    url = f"{API_ROOT}{endpoint}"
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "archetype-release-publisher-dispatch",
        "X-GitHub-Api-Version": API_VERSION,
    }
    if payload is not None:
        data = json.dumps(dict(payload), sort_keys=True, separators=(",", ":")).encode()
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method=method)

    try:
        with open_request(request, timeout=timeout_seconds) as response:
            final_url = response.geturl()
            if final_url != url:
                raise GitHubAPIError(f"GitHub API redirected {endpoint} outside its exact URL")
            status = getattr(response, "status", None)
            if status is None:
                status = response.getcode()
            body = _read_response(response)
    except HTTPError as error:
        try:
            detail = _response_detail(error.read(_MAX_RESPONSE_BYTES + 1))
        except (HTTPException, OSError):
            detail = str(error.reason)
        raise GitHubAPIError(
            f"GitHub API {method} {endpoint} failed with HTTP {error.code}: {detail}"
        ) from error
    except GitHubAPIError:
        raise
    except (HTTPException, OSError, TimeoutError, URLError) as error:
        reason = getattr(error, "reason", error)
        raise GitHubAPIError(f"GitHub API {method} {endpoint} failed: {reason}") from error

    if status != 200:
        raise GitHubAPIError(
            f"GitHub API {method} {endpoint} returned HTTP {status}: {_response_detail(body)}"
        )
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GitHubAPIError(f"GitHub API {method} {endpoint} returned invalid JSON") from error
    if not isinstance(value, dict):
        raise GitHubAPIError(f"GitHub API {method} {endpoint} must return a JSON object")
    return value


def _run_api_url(run_id: int) -> str:
    return f"{API_ROOT}/repos/{REPOSITORY}/actions/runs/{run_id}"


def _run_html_url(run_id: int) -> str:
    return f"https://github.com/{REPOSITORY}/actions/runs/{run_id}"


def _dispatch_response(
    value: Mapping[str, Any],
    *,
    distribution: str,
    workflow: str,
) -> dict[str, Any]:
    run_id = value.get("workflow_run_id")
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id < 1:
        raise PublisherDispatchError(
            f"GitHub did not return an exact run ID for {distribution} via {workflow}"
        )
    expected_api_url = _run_api_url(run_id)
    expected_html_url = _run_html_url(run_id)
    if value.get("run_url") != expected_api_url or value.get("html_url") != expected_html_url:
        raise PublisherDispatchError(
            f"GitHub returned unexpected run URLs for {distribution} via {workflow}"
        )
    return {
        "distribution": distribution,
        "workflow": workflow,
        "run_id": run_id,
        "url": expected_html_url,
    }


def dispatch_publishers(
    *,
    parent_run_id: int,
    parent_run_attempt: int,
    tag: str,
    expected_commit: str,
    expected_tag_object: str,
    registry: str,
    token: str,
    open_request: OpenRequest = _OPEN,
    http_timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Dispatch each child publisher and return its exact-run allowlist."""

    (
        parent_run_id,
        parent_run_attempt,
        tag,
        expected_commit,
        expected_tag_object,
        registry,
    ) = _context(
        parent_run_id=parent_run_id,
        parent_run_attempt=parent_run_attempt,
        tag=tag,
        expected_commit=expected_commit,
        expected_tag_object=expected_tag_object,
        registry=registry,
    )
    token = _token(token)
    http_timeout_seconds = _duration(
        http_timeout_seconds,
        "HTTP timeout",
        MAX_HTTP_TIMEOUT_SECONDS,
    )
    inputs = {
        "parent_run_id": str(parent_run_id),
        "parent_run_attempt": str(parent_run_attempt),
        "tag": tag,
        "expected_commit": expected_commit,
        "expected_tag_object": expected_tag_object,
        "registry": registry,
    }
    runs: list[dict[str, Any]] = []
    observed_run_ids: set[int] = set()
    for distribution, workflow in _publishers():
        endpoint = f"/repos/{REPOSITORY}/actions/workflows/{quote(workflow, safe='')}/dispatches"
        response = _request_json(
            "POST",
            endpoint,
            token=token,
            payload={"ref": tag, "inputs": inputs, "return_run_details": True},
            open_request=open_request,
            timeout_seconds=http_timeout_seconds,
        )
        run = _dispatch_response(
            response,
            distribution=distribution,
            workflow=workflow,
        )
        if run["run_id"] in observed_run_ids:
            raise PublisherDispatchError("GitHub returned a duplicate publisher run ID")
        observed_run_ids.add(run["run_id"])
        runs.append(run)

    return {
        "schema": SCHEMA,
        "repository": REPOSITORY,
        "parent_run_id": parent_run_id,
        "parent_run_attempt": parent_run_attempt,
        "tag": tag,
        "commit": expected_commit,
        "tag_object": expected_tag_object,
        "registry": registry,
        "runs": runs,
    }


def validate_allowlist(
    value: object,
    *,
    parent_run_id: int,
    parent_run_attempt: int,
    tag: str,
    expected_commit: str,
    expected_tag_object: str,
    registry: str,
) -> dict[str, Any]:
    """Validate an immutable dispatch receipt against the parent run context."""

    (
        parent_run_id,
        parent_run_attempt,
        tag,
        expected_commit,
        expected_tag_object,
        registry,
    ) = _context(
        parent_run_id=parent_run_id,
        parent_run_attempt=parent_run_attempt,
        tag=tag,
        expected_commit=expected_commit,
        expected_tag_object=expected_tag_object,
        registry=registry,
    )
    if not isinstance(value, dict):
        raise PublisherDispatchError("publisher dispatch allowlist must be a JSON object")
    expected_keys = {
        "schema",
        "repository",
        "parent_run_id",
        "parent_run_attempt",
        "tag",
        "commit",
        "tag_object",
        "registry",
        "runs",
    }
    if set(value) != expected_keys:
        raise PublisherDispatchError("publisher dispatch allowlist has unexpected fields")
    expected_context = {
        "schema": SCHEMA,
        "repository": REPOSITORY,
        "parent_run_id": parent_run_id,
        "parent_run_attempt": parent_run_attempt,
        "tag": tag,
        "commit": expected_commit,
        "tag_object": expected_tag_object,
        "registry": registry,
    }
    for field, expected in expected_context.items():
        if value.get(field) != expected:
            raise PublisherDispatchError(
                f"publisher dispatch allowlist has unexpected {field.replace('_', ' ')}"
            )

    raw_runs = value.get("runs")
    publishers = _publishers()
    if not isinstance(raw_runs, list) or len(raw_runs) != len(publishers):
        raise PublisherDispatchError("publisher dispatch allowlist has the wrong run count")
    runs: list[dict[str, Any]] = []
    observed_run_ids: set[int] = set()
    run_keys = {"distribution", "workflow", "run_id", "url"}
    for raw, (distribution, workflow) in zip(raw_runs, publishers, strict=True):
        if not isinstance(raw, dict) or set(raw) != run_keys:
            raise PublisherDispatchError("publisher dispatch run has unexpected fields")
        if raw.get("distribution") != distribution or raw.get("workflow") != workflow:
            raise PublisherDispatchError("publisher dispatch runs are not in distribution order")
        run_id = raw.get("run_id")
        if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id < 1:
            raise PublisherDispatchError(f"publisher dispatch run for {distribution} has no ID")
        if run_id in observed_run_ids:
            raise PublisherDispatchError("publisher dispatch allowlist contains a duplicate run ID")
        if raw.get("url") != _run_html_url(run_id):
            raise PublisherDispatchError(
                f"publisher dispatch run for {distribution} has a wrong URL"
            )
        observed_run_ids.add(run_id)
        runs.append(
            {
                "distribution": distribution,
                "workflow": workflow,
                "run_id": run_id,
                "url": _run_html_url(run_id),
            }
        )

    return {**expected_context, "runs": runs}


def _validate_workflow_run(
    value: Mapping[str, Any],
    *,
    distribution: str,
    workflow: str,
    run_id: int,
    tag: str,
    expected_commit: str,
) -> bool:
    workflow_path = f".github/workflows/{workflow}"
    allowed_paths = {
        workflow_path,
        f"{workflow_path}@{tag}",
        f"{workflow_path}@refs/tags/{tag}",
    }
    expected = {
        "id": run_id,
        "event": "workflow_dispatch",
        "head_sha": expected_commit,
        "head_branch": tag,
        "url": _run_api_url(run_id),
        "html_url": _run_html_url(run_id),
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise PublisherDispatchError(
                f"publisher run {run_id} for {distribution} has unexpected {field}"
            )
    if value.get("path") not in allowed_paths:
        raise PublisherDispatchError(
            f"publisher run {run_id} for {distribution} has unexpected path"
        )
    repository = value.get("repository")
    if not isinstance(repository, dict) or repository.get("full_name") != REPOSITORY:
        raise PublisherDispatchError(
            f"publisher run {run_id} for {distribution} has unexpected repository"
        )

    status = value.get("status")
    conclusion = value.get("conclusion")
    if status == "completed":
        if conclusion != "success":
            raise PublisherDispatchError(
                f"publisher run {run_id} for {distribution} completed with {conclusion!r}"
            )
        return True
    if status not in _IN_PROGRESS_STATUSES or conclusion is not None:
        raise PublisherDispatchError(
            f"publisher run {run_id} for {distribution} has invalid state "
            f"status={status!r}, conclusion={conclusion!r}"
        )
    return False


def await_publishers(
    allowlist: object,
    *,
    parent_run_id: int,
    parent_run_attempt: int,
    tag: str,
    expected_commit: str,
    expected_tag_object: str,
    registry: str,
    token: str,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    http_timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    open_request: OpenRequest = _OPEN,
    sleep: Sleep = time.sleep,
    monotonic: Monotonic = time.monotonic,
) -> dict[str, Any]:
    """Poll only allowlisted child run IDs and require every run to succeed."""

    receipt = validate_allowlist(
        allowlist,
        parent_run_id=parent_run_id,
        parent_run_attempt=parent_run_attempt,
        tag=tag,
        expected_commit=expected_commit,
        expected_tag_object=expected_tag_object,
        registry=registry,
    )
    token = _token(token)
    timeout_seconds = _duration(timeout_seconds, "poll timeout", MAX_TIMEOUT_SECONDS)
    interval_seconds = _duration(interval_seconds, "poll interval", MAX_INTERVAL_SECONDS)
    http_timeout_seconds = _duration(
        http_timeout_seconds,
        "HTTP timeout",
        MAX_HTTP_TIMEOUT_SECONDS,
    )
    pending = {int(run["run_id"]): run for run in receipt["runs"]}
    deadline = monotonic() + timeout_seconds

    while pending:
        for run_id, run in tuple(pending.items()):
            remaining = deadline - monotonic()
            if remaining <= 0:
                names = ", ".join(str(value["distribution"]) for value in pending.values())
                raise PublisherDispatchError(
                    f"publisher runs did not complete within {timeout_seconds:g} seconds: {names}"
                )
            value = _request_json(
                "GET",
                f"/repos/{REPOSITORY}/actions/runs/{run_id}",
                token=token,
                open_request=open_request,
                timeout_seconds=min(http_timeout_seconds, remaining),
            )
            if _validate_workflow_run(
                value,
                distribution=str(run["distribution"]),
                workflow=str(run["workflow"]),
                run_id=run_id,
                tag=tag,
                expected_commit=expected_commit,
            ):
                del pending[run_id]

        if pending:
            remaining = deadline - monotonic()
            if remaining <= 0:
                names = ", ".join(str(value["distribution"]) for value in pending.values())
                raise PublisherDispatchError(
                    f"publisher runs did not complete within {timeout_seconds:g} seconds: {names}"
                )
            sleep(min(interval_seconds, remaining))

    return receipt


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise PublisherDispatchError(
            f"could not read publisher allowlist {path}: {error}"
        ) from error
    except json.JSONDecodeError as error:
        raise PublisherDispatchError(f"publisher allowlist {path} is not valid JSON") from error
    if not isinstance(value, dict):
        raise PublisherDispatchError("publisher dispatch allowlist must be a JSON object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(dict(value), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _positive_decimal(value: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise argparse.ArgumentTypeError("must be a positive decimal integer")
    return int(value)


def _add_context_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--parent-run-id", type=_positive_decimal, required=True)
    parser.add_argument("--parent-run-attempt", type=_positive_decimal, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-tag-object", required=True)
    parser.add_argument("--registry", choices=sorted(_REGISTRIES), required=True)
    parser.add_argument("--token-env", default="GITHUB_TOKEN")
    parser.add_argument(
        "--http-timeout-seconds",
        type=float,
        default=DEFAULT_HTTP_TIMEOUT_SECONDS,
    )
    parser.add_argument("--out", type=Path, required=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    dispatch_parser = subparsers.add_parser("dispatch", help="dispatch and record exact run IDs")
    _add_context_arguments(dispatch_parser)
    await_parser = subparsers.add_parser("await", help="await only the recorded run IDs")
    _add_context_arguments(await_parser)
    await_parser.add_argument("--allowlist", type=Path, required=True)
    await_parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    await_parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    args = parser.parse_args(argv)

    token = os.environ.get(args.token_env)
    if token is None:
        parser.error(f"GitHub token environment variable {args.token_env!r} is not set")
    if args.command == "dispatch":
        receipt = dispatch_publishers(
            parent_run_id=args.parent_run_id,
            parent_run_attempt=args.parent_run_attempt,
            tag=args.tag,
            expected_commit=args.expected_commit,
            expected_tag_object=args.expected_tag_object,
            registry=args.registry,
            token=token,
            http_timeout_seconds=args.http_timeout_seconds,
        )
    else:
        receipt = await_publishers(
            _load_json(args.allowlist),
            parent_run_id=args.parent_run_id,
            parent_run_attempt=args.parent_run_attempt,
            tag=args.tag,
            expected_commit=args.expected_commit,
            expected_tag_object=args.expected_tag_object,
            registry=args.registry,
            token=token,
            http_timeout_seconds=args.http_timeout_seconds,
            timeout_seconds=args.timeout_seconds,
            interval_seconds=args.interval_seconds,
        )
    _write_json(args.out, receipt)
    print(
        f"Publisher {args.command} passed for {len(receipt['runs'])} child runs "
        f"on {receipt['registry']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
