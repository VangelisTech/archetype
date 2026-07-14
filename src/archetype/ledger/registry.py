# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Trusted in-process component identity and schema resolution."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.interfaces import ArchetypeSignature
from archetype.ledger.canonical import (
    COMPONENT_SERIALIZER_VERSION,
    arrow_schema_descriptor,
    component_schema_digest,
    composite_schema_digest,
    internal_digest,
)
from archetype.ledger.errors import ComponentResolutionError, ComponentSchemaConflictError
from archetype.ledger.models import ComponentRef, SignatureRef


@dataclass(frozen=True, slots=True)
class _Registration:
    component_type: type[Component]
    ref: ComponentRef
    serializer_version: str


class ComponentRegistry:
    """Resolve persisted component IDs only through explicitly trusted types.

    The registry never imports a module named by persisted state.  Callers must
    register already-imported component classes before resolving a reference.
    """

    def __init__(self) -> None:
        self._by_id: dict[str, _Registration] = {}
        self._by_type: dict[type[Component], _Registration] = {}

    @staticmethod
    def default_component_id(component_type: type[Component]) -> str:
        module = component_type.__module__
        qualname = component_type.__qualname__
        if module == "__main__" or "." in qualname or "<locals>" in qualname:
            raise ComponentResolutionError(
                "local, nested, and __main__ component classes require an explicit stable "
                f"component_id: {module}.{qualname}"
            )
        return f"python:{module}:{qualname}"

    def register(
        self,
        component_type: type[Component],
        *,
        component_id: str | None = None,
        serializer_version: str = COMPONENT_SERIALIZER_VERSION,
    ) -> ComponentRef:
        if not isinstance(component_type, type) or not issubclass(component_type, Component):
            raise TypeError("component_type must be a Component subclass")
        stable_id = (
            self.default_component_id(component_type) if component_id is None else component_id
        )
        schema_digest = component_schema_digest(
            stable_id,
            component_type.get_prefixed_schema(),
            serializer_version=serializer_version,
        )
        ref = ComponentRef(component_id=stable_id, schema_digest=schema_digest)

        existing_by_id = self._by_id.get(stable_id)
        if existing_by_id is not None:
            if existing_by_id.ref.schema_digest != ref.schema_digest:
                raise ComponentSchemaConflictError(
                    stable_id,
                    existing_by_id.ref.schema_digest,
                    ref.schema_digest,
                )
            if existing_by_id.component_type is not component_type:
                raise ComponentResolutionError(
                    f"component_id {stable_id!r} is already bound to "
                    f"{existing_by_id.component_type.__module__}."
                    f"{existing_by_id.component_type.__qualname__}"
                )
            return existing_by_id.ref

        existing_by_type = self._by_type.get(component_type)
        if existing_by_type is not None and existing_by_type.ref.component_id != stable_id:
            raise ComponentResolutionError(
                f"component type {component_type.__module__}.{component_type.__qualname__} "
                f"is already registered as {existing_by_type.ref.component_id!r}"
            )

        registration = _Registration(component_type, ref, serializer_version)
        self._by_id[stable_id] = registration
        self._by_type[component_type] = registration
        return ref

    def component_ref(self, component_type: type[Component]) -> ComponentRef:
        registration = self._by_type.get(component_type)
        if registration is None:
            raise ComponentResolutionError(
                f"component type {component_type.__module__}.{component_type.__qualname__} "
                "is not registered"
            )
        return registration.ref

    def resolve(self, ref: ComponentRef | str) -> type[Component]:
        component_id = ref.component_id if isinstance(ref, ComponentRef) else ref
        registration = self._by_id.get(component_id)
        if registration is None:
            raise ComponentResolutionError(
                f"component_id {component_id!r} is absent from the trusted registry"
            )
        if isinstance(ref, ComponentRef) and registration.ref.schema_digest != ref.schema_digest:
            raise ComponentSchemaConflictError(
                component_id,
                ref.schema_digest,
                registration.ref.schema_digest,
            )
        return registration.component_type

    def signature_ref(self, component_types: Sequence[type[Component]]) -> SignatureRef:
        if not component_types:
            raise ValueError("signature requires at least one component type")
        if len(component_types) != len(set(component_types)):
            raise ValueError("signature component types must be unique")

        registrations = []
        for component_type in component_types:
            registration = self._by_type.get(component_type)
            if registration is None:
                raise ComponentResolutionError(
                    f"component type {component_type.__module__}.{component_type.__qualname__} "
                    "is not registered"
                )
            registrations.append(registration)
        registrations.sort(key=lambda registration: registration.ref.component_id)

        canonical_refs = tuple(registration.ref for registration in registrations)

        # Existing physical tables are named and laid out in legacy class-name
        # order.  Component references remain sorted by stable component ID,
        # but the signature's schema identity must describe the actual table
        # named by ``table_id`` or a legitimate cold read cannot verify it.
        legacy_types: ArchetypeSignature = tuple(
            sorted(component_types, key=lambda value: value.__name__)
        )
        composite_schema = Archetype.get_archetype_schema(legacy_types)
        schema_digest = composite_schema_digest(composite_schema)
        signature_digest = internal_digest(
            "archetype-signature-v1",
            {
                "components": [ref.model_dump(mode="json") for ref in canonical_refs],
                "schema": arrow_schema_descriptor(composite_schema),
            },
        )

        # Physical table names remain exactly compatible with the existing
        # class-name ordered Archetype implementation.
        table_id = Archetype.get_name(legacy_types)
        return SignatureRef(
            table_id=table_id,
            components=canonical_refs,
            signature_digest=signature_digest,
            schema_digest=schema_digest,
        )

    def resolve_signature(self, ref: SignatureRef) -> ArchetypeSignature:
        component_types = tuple(self.resolve(component_ref) for component_ref in ref.components)
        recomputed = self.signature_ref(component_types)
        if recomputed.schema_digest != ref.schema_digest:
            raise ComponentSchemaConflictError(
                f"signature:{ref.signature_digest}", ref.schema_digest, recomputed.schema_digest
            )
        if recomputed.signature_digest != ref.signature_digest:
            raise ComponentResolutionError(
                f"signature digest mismatch: expected {ref.signature_digest}, "
                f"got {recomputed.signature_digest}"
            )
        if recomputed.table_id != ref.table_id:
            raise ComponentResolutionError(
                f"signature table mismatch: expected {ref.table_id!r}, got {recomputed.table_id!r}"
            )
        return component_types

    def registered_refs(self) -> tuple[ComponentRef, ...]:
        return tuple(
            registration.ref
            for registration in sorted(
                self._by_id.values(), key=lambda registration: registration.ref.component_id
            )
        )


def signature_schema(types: Sequence[type[Component]]) -> pa.Schema:
    """Expose the semantic component-ID ordering only through a registry."""

    return Archetype.get_archetype_schema(tuple(types))
