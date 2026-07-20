#!/usr/bin/env python3
"""Fail when artifact durability bypasses the shared redaction authority."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGET = ROOT / "src" / "archetype" / "app" / "artifacts" / "bundle_service.py"
TRANSCRIPT_TARGET = ROOT / "src" / "archetype" / "app" / "artifacts" / "transcript_service.py"


def _service_methods(tree: ast.Module) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "ArtifactBundleService":
            return {
                child.name: child
                for child in node.body
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
    return {}


def _transcript_methods(tree: ast.Module) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "ClaudeTranscriptIngestionService":
            return {
                child.name: child
                for child in node.body
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
    return {}


def _call_lines(node: ast.AST, name: str) -> list[int]:
    # ``asyncio.to_thread(self._sanitize_materialized, ...)`` passes a bound
    # method rather than calling it syntactically. Attribute-use ordering is
    # therefore the correct audit surface for both direct and to-thread calls.
    return sorted(
        child.lineno
        for child in ast.walk(node)
        if (isinstance(child, ast.Attribute) and child.attr == name)
        or (isinstance(child, ast.Name) and child.id == name)
    )


def _require_order(
    errors: list[str],
    method: ast.AST,
    method_name: str,
    first: str,
    second: str,
) -> None:
    first_lines = _call_lines(method, first)
    second_lines = _call_lines(method, second)
    if not first_lines:
        errors.append(f"{method_name} must call {first}()")
    if not second_lines:
        errors.append(f"{method_name} must call {second}()")
    if first_lines and second_lines and first_lines[0] >= second_lines[0]:
        errors.append(f"{method_name} must call {first}() before {second}()")


def audit_path(path: Path = DEFAULT_TARGET) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    methods = _service_methods(tree)
    errors: list[str] = []
    required = {
        "prepare",
        "publish",
        "publish_prepared",
        "reconcile",
        "reconcile_publication",
        "_resume",
        "_upload_bundle",
    }
    missing = sorted(required - methods.keys())
    errors.extend(f"ArtifactBundleService is missing {name}()" for name in missing)
    if missing:
        return errors

    prepare = methods["prepare"]
    publish = methods["publish"]
    publish_prepared = methods["publish_prepared"]
    reconcile_publication = methods["reconcile_publication"]
    resume = methods["_resume"]
    upload_bundle = methods["_upload_bundle"]
    _require_order(
        errors,
        prepare,
        "prepare",
        "_bind_redaction_policy",
        "canonical_json",
    )
    _require_order(
        errors,
        publish,
        "publish",
        "prepare",
        "publish_prepared",
    )
    _require_order(
        errors,
        publish_prepared,
        "publish_prepared",
        "_request_from_preparation",
        "_control_catalog",
    )
    _require_order(
        errors,
        publish_prepared,
        "publish_prepared",
        "_safe_failure_detail",
        "fail_artifact_publication",
    )
    _require_order(
        errors,
        reconcile_publication,
        "reconcile_publication",
        "_safe_failure_detail",
        "fail_artifact_publication",
    )
    _require_order(
        errors,
        resume,
        "_resume",
        "_assert_object_root_safe",
        "_upload_bundle",
    )
    _require_order(
        errors,
        resume,
        "_resume",
        "_assert_object_root_safe",
        "_index_records",
    )
    _require_order(
        errors,
        upload_bundle,
        "_upload_bundle",
        "_assert_materialized_metadata_safe",
        "_upload_files",
    )
    _require_order(
        errors,
        upload_bundle,
        "_upload_bundle",
        "_sanitize_materialized",
        "_file_metadata",
    )
    _require_order(
        errors,
        upload_bundle,
        "_upload_bundle",
        "_file_metadata",
        "_upload_files",
    )
    _require_order(
        errors,
        upload_bundle,
        "_upload_bundle",
        "_redaction_manifest",
        "_upload_bytes",
    )
    _require_order(
        errors,
        upload_bundle,
        "_upload_bundle",
        "_upload_bytes",
        "_assert_records_safe",
    )
    return errors


def audit_transcript_path(path: Path = TRANSCRIPT_TARGET) -> list[str]:
    """Prove transcript source bytes cross safety checks before durable writers."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    methods = _transcript_methods(tree)
    ingest = methods.get("ingest")
    if ingest is None:
        return ["ClaudeTranscriptIngestionService is missing ingest()"]

    errors: list[str] = []
    _require_order(errors, ingest, "transcript ingest", "assert_safe_metadata", "sanitize_file")
    _require_order(errors, ingest, "transcript ingest", "sanitize_file", "read_text")
    _require_order(
        errors,
        ingest,
        "transcript ingest",
        "sanitize_file",
        "parse_claude_transcript",
    )
    _require_order(
        errors,
        ingest,
        "transcript ingest",
        "parse_claude_transcript",
        "_normalize_session",
    )
    _require_order(errors, ingest, "transcript ingest", "_normalize_session", "publish")
    _require_order(errors, ingest, "transcript ingest", "publish", "_artifact_rows")
    _require_order(errors, ingest, "transcript ingest", "_artifact_rows", "write_artifacts")

    raw_reads = [
        node.lineno
        for node in ast.walk(ingest)
        if isinstance(node, ast.Attribute)
        and node.attr in {"read_text", "read_bytes", "open"}
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "path"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "source"
    ]
    if raw_reads:
        errors.append("transcript ingest must never read source.path before sanitization")
    return errors


def main() -> int:
    errors = [*audit_path(), *audit_transcript_path()]
    if errors:
        print("Pre-durability redaction audit failed:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print("Pre-durability redaction audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
