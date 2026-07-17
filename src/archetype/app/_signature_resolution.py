# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Resolve durable signature records to imported component classes.

The control catalog stores component names and a schema fingerprint, not
Python import paths.  A process may also have several same-named component
classes loaded (tests, plugins, or an evolved definition), so names alone are
not identity.  This module is the single app-layer boundary that joins a
durable record back to an interchangeable, schema-identical class tuple.
"""

from __future__ import annotations

from collections.abc import Iterable
from itertools import product

from archetype.app._catalog import SignatureRecord, schema_fingerprint
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.interfaces import ArchetypeSignature


def _component_classes_by_name() -> dict[str, list[type[Component]]]:
    """Return every imported Component subclass, grouped by class name."""
    classes: dict[str, list[type[Component]]] = {}
    stack: list[type[Component]] = list(Component.__subclasses__())
    seen: set[type[Component]] = set()
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        stack.extend(cls.__subclasses__())
        classes.setdefault(cls.__name__, []).append(cls)
    return classes


def match_signature_records(
    records: Iterable[SignatureRecord],
) -> tuple[dict[str, ArchetypeSignature], dict[str, str]]:
    """Partition catalog records into exact matches and diagnosed problems.

    Multiple imported classes with the same name are harmless when their
    complete archetype schema and table identity match the durable record.
    A missing or drifted definition is not guessed because doing so would
    make code, rather than the stored schema, silently reinterpret rows.

    The non-raising partition lets discovery skip unrelated historical drift
    while strict lifecycle callers use :func:`resolve_signature_records`.
    """
    available = _component_classes_by_name()
    resolved: dict[str, ArchetypeSignature] = {}
    problems: dict[str, str] = {}

    for record in records:
        if record.table_id in resolved or record.table_id in problems:
            continue

        missing = sorted(name for name in record.component_names if not available.get(name))
        if missing:
            problems[record.table_id] = f"component class(es) {', '.join(missing)} are not imported"
            continue

        matches: list[ArchetypeSignature] = []
        mismatched_table_ids: set[str] = set()
        candidates = (
            sorted(available[name], key=lambda cls: (cls.__module__, cls.__qualname__))
            for name in record.component_names
        )
        for combination in product(*candidates):
            signature = tuple(sorted(set(combination), key=lambda cls: cls.__name__))
            try:
                schema = Archetype.get_archetype_schema(signature)
                fingerprint = schema_fingerprint(schema)
                table_id = Archetype.get_name(signature)
            except Exception:
                continue
            if fingerprint != record.fingerprint:
                continue
            if table_id != record.table_id:
                mismatched_table_ids.add(table_id)
                continue
            if signature not in matches:
                matches.append(signature)

        if matches:
            # Candidate ordering makes the representative deterministic.
            # Every match is interchangeable by the complete fingerprint.
            resolved[record.table_id] = matches[0]
        elif mismatched_table_ids:
            rendered = ", ".join(sorted(mismatched_table_ids))
            problems[record.table_id] = (
                "schema-compatible imported class combination resolves to different "
                f"table identity: {rendered}"
            )
        else:
            problems[record.table_id] = (
                "no imported class combination matches the stored schema "
                "(the definitions may have drifted since the rows were written)"
            )

    return resolved, problems


def resolve_signature_records(
    records: Iterable[SignatureRecord],
    *,
    operation: str,
) -> dict[str, ArchetypeSignature]:
    """Resolve every record exactly or fail the bounded lifecycle operation."""
    resolved, problems = match_signature_records(records)

    if problems:
        detail = "; ".join(
            f"table {table_id}: {message}" for table_id, message in sorted(problems.items())
        )
        raise RuntimeError(
            f"cannot {operation}: {detail} (code is not rows — import the exact component "
            "definitions before reading durable signatures)"
        )

    return resolved
