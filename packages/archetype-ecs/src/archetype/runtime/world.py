# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Narrow immutable handles. Native identity and diagnostics remain private."""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from archetype_native import wire as w
from archetype_native.programs import Composition, LeafProgram, ProgramReference
from archetype_native.values import ARTIFACT_INDEX_FIELDS, decode_float_bits, float_bits, identifier

from archetype.runtime.contracts import (
    Admission,
    ArtifactContextInfo,
    ArtifactOccurrence,
    ArtifactPage,
    ArtifactUploadReceipt,
    PreparedArtifacts,
    RowPage,
    WorldStatus,
)

if TYPE_CHECKING:
    from archetype.runtime.runtime import ArchetypeRuntime, SyncArchetypeRuntime


def _cell(value: int | str | bool | float) -> dict[str, Any]:
    if type(value) is bool:
        return {"bool": value}
    if type(value) is int:
        return {"int64": str(value)}
    if type(value) is str:
        return {"string": value}
    if type(value) is float:
        return {"float64": float_bits(value)}
    raise ValueError("Expected an exact supported live cell")


def _facts(raw):
    def value(cell):
        if cell is None:
            return None
        tag, fact = next(iter(cell.items()))
        if tag == "timestamp_us":
            return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=int(fact))
        if tag == "int64":
            return int(fact)
        if tag == "float64":
            return decode_float_bits(fact)
        return fact

    return tuple((name, value(cell)) for name, cell in raw.items())


@dataclass(frozen=True, slots=True)
class Change:
    predicate: str
    values: tuple[int | str | bool | float, ...]
    op: str = "insert"

    def __post_init__(self) -> None:
        identifier(self.predicate)
        if type(self.values) is not tuple:
            raise ValueError("Expected immutable change values")
        w.Change.decode(self._wire())

    def _wire(self) -> dict[str, Any]:
        return {
            "predicate": self.predicate,
            "op": self.op,
            "values": [_cell(value) for value in self.values],
        }


@dataclass(frozen=True, slots=True)
class RuntimeProgram:
    _runtime: ArchetypeRuntime = field(repr=False, compare=False)
    name: str

    async def publish(
        self, definition: LeafProgram | Composition, *, request_key: str, description: str = ""
    ) -> ProgramReference:
        if type(definition) is LeafProgram:
            args = {
                "definition": {
                    "rules": definition.rules,
                    "schemas": [
                        {"name": r.name, "input": r.input, "fields": list(r.types)}
                        for r in definition.schemas
                    ],
                    "inputs": list(definition.inputs),
                    "outputs": list(definition.outputs),
                }
            }
            operation = "program_create"
        elif type(definition) is Composition:
            args = {
                "composition": {
                    "nodes": [
                        {
                            "name": n.name,
                            "program": {"resource": n.program.resource, **n.program.pin()},
                        }
                        for n in definition.nodes
                    ],
                    "inputs": [
                        {
                            "name": p.name,
                            "fields": list(p.types),
                            "targets": [t.native() for t in p.targets],
                        }
                        for p in definition.inputs
                    ],
                    "bindings": [
                        {"from": b.source.native(), "to": b.target.native()}
                        for b in definition.bindings
                    ],
                    "outputs": [
                        {"name": p.name, "source": p.source.native()} for p in definition.outputs
                    ],
                }
            }
            operation = "program_compose"
        else:
            raise ValueError("Expected immutable program or composition")
        result = await self._runtime._invoke(
            self.name, operation, {**args, "request_key": request_key, "description": description}
        )
        return ProgramReference.decode(result["program"])

    async def resolve(self) -> ProgramReference:
        result = await self._runtime._invoke(self.name, "program_resolve", {})
        return ProgramReference.decode(result["program"])


@dataclass(frozen=True, slots=True)
class RuntimeCut:
    _runtime: ArchetypeRuntime = field(repr=False, compare=False)
    _resource: str = field(repr=False)
    world: str
    run: str
    tick: int
    cut_id: str
    parent: str | None = None

    def _receipt(self) -> dict[str, Any]:
        return {"world": self.world, "run": self.run, "tick": str(self.tick), "cut_id": self.cut_id}

    async def read(self, component: str, *, offset: int = 0, limit: int = 32) -> RowPage:
        value = await self._runtime._invoke(
            self._resource,
            "read",
            {
                "receipt": self._receipt(),
                "component": component,
                "offset": str(offset),
                "limit": str(limit),
            },
        )
        return RowPage(
            tuple(value["fields"]),
            tuple(tuple(w.Cell.decode(c).value for c in row) for row in value["rows"]),
            int(value["total_rows"]),
            None if value["next_offset"] is None else int(value["next_offset"]),
        )

    async def analyze(self, component: str, *, offset: int = 0, limit: int = 32):
        """Optional lazy Daft frame for a bounded immutable page, outside ticks."""
        page = await self.read(component, offset=offset, limit=limit)
        import daft

        return daft.from_pydict(
            {name: [row[i] for row in page.rows] for i, name in enumerate(page.fields)}
        )


@dataclass(frozen=True, slots=True)
class CutPage[T]:
    cuts: tuple[T, ...]
    total: int
    next_offset: int | None


@dataclass(frozen=True, slots=True)
class RuntimeWorld:
    _runtime: ArchetypeRuntime = field(repr=False, compare=False)
    name: str
    world: str
    run: str

    async def create(
        self, program: ProgramReference, *, request_key: str, label: str | None = None
    ) -> WorldStatus:
        if type(program) is not ProgramReference:
            raise ValueError("Expected exact logical program reference")
        value = await self._runtime._invoke(
            self.name,
            "create",
            {
                "program": {"resource": program.resource, **program.pin()},
                "request_key": request_key,
                "label": self.name if label is None else label,
            },
        )
        return WorldStatus._decode(value)

    async def status(self) -> WorldStatus:
        return WorldStatus._decode(await self._runtime._invoke(self.name, "status", {}))

    async def start(self) -> WorldStatus:
        return WorldStatus._decode(await self._runtime._invoke(self.name, "start", {}))

    async def stop(self) -> WorldStatus:
        return WorldStatus._decode(await self._runtime._invoke(self.name, "stop", {}))

    async def admit(
        self,
        changes: tuple[Change, ...],
        *,
        generation: int,
        revision: int,
        admission_key: str,
        expected_head: str | None,
    ) -> Admission:
        if type(changes) is not tuple or any(type(c) is not Change for c in changes):
            raise ValueError("Expected immutable typed changes")
        value = await self._runtime._invoke(
            self.name,
            "admit",
            {
                "changes": [c._wire() for c in changes],
                "generation": str(generation),
                "revision": str(revision),
                "admission_key": admission_key,
                "expected_head": expected_head,
            },
        )
        return Admission._decode(value)

    async def admission_status(self, generation: int, admission_key: str) -> Admission:
        return Admission._decode(
            await self._runtime._invoke(
                self.name,
                "admission_status",
                {"generation": str(generation), "admission_key": admission_key},
            )
        )

    def _cut(self, value: dict[str, Any]) -> RuntimeCut:
        receipt = value["receipt"]
        return RuntimeCut(
            self._runtime,
            self.name,
            receipt["world"],
            receipt["run"],
            int(receipt["tick"]),
            receipt["cut_id"],
            value["parent"],
        )

    async def publish(self, boundary: w.Boundary) -> RuntimeCut:
        return self._cut(
            await self._runtime._invoke(
                self.name,
                "publish",
                {
                    "boundary": {
                        "generation": str(boundary.generation),
                        "admission_key": boundary.admission_key,
                        "request_sha256": boundary.request_sha256,
                    }
                },
            )
        )

    async def reconcile(
        self, boundary: w.Boundary, *, tick: int, expected_parent: str | None
    ) -> RuntimeCut:
        return self._cut(
            await self._runtime._invoke(
                self.name,
                "reconcile",
                {
                    "boundary": {
                        "generation": str(boundary.generation),
                        "admission_key": boundary.admission_key,
                        "request_sha256": boundary.request_sha256,
                    },
                    "tick": str(tick),
                    "expected_parent": expected_parent,
                },
            )
        )

    async def confirm(self, boundary: w.Boundary, cut: RuntimeCut) -> Admission:
        if (cut.world, cut.run) != (self.world, self.run):
            raise ValueError("Cut outside world scope")
        return Admission._decode(
            await self._runtime._invoke(
                self.name,
                "confirm",
                {
                    "boundary": {
                        "generation": str(boundary.generation),
                        "admission_key": boundary.admission_key,
                        "request_sha256": boundary.request_sha256,
                    },
                    "tick": str(cut.tick),
                    "expected_parent": cut.parent,
                },
            )
        )

    async def history(self, *, offset: int = 0, limit: int = 32) -> CutPage[RuntimeCut]:
        value = await self._runtime._invoke(
            self.name, "history", {"offset": str(offset), "limit": str(limit)}
        )
        return CutPage(
            tuple(self._cut(item) for item in value["receipts"]),
            int(value["total"]),
            None if value["next_offset"] is None else int(value["next_offset"]),
        )

    async def resume(self, cut: RuntimeCut, *, expected_generation: int) -> WorldStatus:
        return WorldStatus._decode(
            await self._runtime._invoke(
                self.name,
                "restore",
                {"receipt": cut._receipt(), "expected_generation": str(expected_generation)},
            )
        )

    async def fork(
        self,
        source: RuntimeWorld,
        cut: RuntimeCut,
        *,
        request_key: str,
        expected_generation: int = 0,
    ) -> WorldStatus:
        if source._runtime is not self._runtime or (cut.world, cut.run) != (
            source.world,
            source.run,
        ):
            raise ValueError("Fork source must belong to this runtime and exact world")
        return WorldStatus._decode(
            await self._runtime._invoke(
                self.name,
                "fork",
                {
                    "source_resource": source.name,
                    "receipt": cut._receipt(),
                    "request_key": request_key,
                    "expected_generation": str(expected_generation),
                },
            )
        )

    def artifacts(self, name: str):
        return self._runtime.artifacts(name, world=self.world, run=self.run, source=self)

    async def shutdown(self) -> None:
        await self._runtime._shutdown_world(self.name)


@dataclass(frozen=True, slots=True)
class RuntimeArtifacts:
    _runtime: ArchetypeRuntime = field(repr=False, compare=False)
    name: str
    world: str
    run: str

    async def publish(self) -> ArtifactContextInfo:
        return ArtifactContextInfo(
            **await self._runtime._invoke(
                self.name,
                "publish_context",
                {"source_resource": self._runtime._resources[self.name].source_resource},
            )
        )

    async def context(self) -> ArtifactContextInfo:
        return ArtifactContextInfo(**await self._runtime._invoke(self.name, "read_context", {}))

    async def upload(
        self, content: bytes, *, logical_path: str, artifact_id: str, cut: RuntimeCut | None = None
    ) -> ArtifactUploadReceipt:
        if type(content) is not bytes or len(content) > 32768:
            raise ValueError("Expected at most 32 KiB of exact bytes")
        if cut is not None and (
            cut._runtime is not self._runtime or (cut.world, cut.run) != (self.world, self.run)
        ):
            raise ValueError("Artifact cut outside context scope")
        context = await self.context()
        value = await self._runtime._invoke(
            self.name,
            "artifact_upload",
            {
                "context_id": context.context_id,
                "exact_cut": None if cut is None else {"tick": str(cut.tick), "cut_id": cut.cut_id},
                "artifact_id": artifact_id,
                "logical_path": logical_path,
                "content_base64": base64.b64encode(content).decode(),
            },
        )
        return ArtifactUploadReceipt(
            value["artifact_id"],
            value["context_id"],
            None
            if value["exact_cut"] is None
            else (int(value["exact_cut"]["tick"]), value["exact_cut"]["cut_id"]),
            value["sha256"],
            value["logical_path"],
            int(value["size_bytes"]),
        )

    async def prepare_files(
        self, sources: tuple, *, cut: RuntimeCut | None = None
    ) -> PreparedArtifacts:
        """Prepare the existing file/batch graph outside live execution.

        Requires the analysis extra. Publication permits at most 32 occurrences,
        64 MiB per content object and 256 MiB aggregate native read admission.
        Retain this immutable batch to retry publication with identical metadata.
        """
        from archetype.artifacts.models import ArtifactSource

        if (
            type(sources) is not tuple
            or not sources
            or len(sources) > 32
            or any(type(source) is not ArtifactSource for source in sources)
        ):
            raise ValueError("Expected 1..32 immutable ArtifactSource declarations")
        if cut is not None and (
            cut._runtime is not self._runtime or (cut.world, cut.run) != (self.world, self.run)
        ):
            raise ValueError("Artifact cut outside context scope")
        context = await self.context()
        exact = None if cut is None else (cut.tick, cut.cut_id)
        prepared = await self._runtime._artifact_files(
            self.name, "prepare_files", self.world, self.run, context.context_id, exact, sources
        )
        return PreparedArtifacts(
            context.context_id,
            exact,
            tuple(item.artifact_id for item in prepared.artifacts),
            prepared,
        )

    async def publish_files(self, prepared: PreparedArtifacts) -> tuple[ArtifactUploadReceipt, ...]:
        """Publish one retained preparation; native storage verifies exact attribution."""
        if type(prepared) is not PreparedArtifacts:
            raise ValueError("Expected an immutable prepared artifact batch")
        context = await self.context()
        target = prepared._prepared.target
        exact = (
            None if target.exact_cut is None else (target.exact_cut.tick, target.exact_cut.cut_id)
        )
        if (
            (target.context.world, target.context.run, target.context.context_id, exact)
            != (self.world, self.run, context.context_id, prepared.exact_cut)
            or context.context_id != prepared.context_id
            or tuple(item.artifact_id for item in prepared._prepared.artifacts)
            != prepared.artifact_ids
        ):
            raise ValueError("Prepared artifact scope mismatch")
        receipts = await self._runtime._artifact_files(
            self.name, "publish_files", prepared._prepared
        )
        if len(receipts) != len(prepared.artifact_ids):
            raise ValueError("Incomplete artifact publication")
        return tuple(
            ArtifactUploadReceipt(
                item.artifact_id,
                context.context_id,
                exact,
                item.sha256,
                item.logical_path,
                item.size_bytes,
            )
            for item in prepared._prepared.artifacts
        )

    async def occurrences(
        self, *, cut: RuntimeCut | None = None, all: bool = False, offset: int = 0, limit: int = 32
    ) -> ArtifactPage:
        if cut is not None and (
            cut._runtime is not self._runtime or (cut.world, cut.run) != (self.world, self.run)
        ):
            raise ValueError("Artifact cut outside context scope")
        context = await self.context()
        value = await self._runtime._invoke(
            self.name,
            "context_artifacts",
            {
                "context_id": context.context_id,
                "exact_cut": None if cut is None else {"tick": str(cut.tick), "cut_id": cut.cut_id},
                "all": all,
                "offset": str(offset),
                "limit": str(limit),
            },
        )
        return ArtifactPage(
            tuple(
                ArtifactOccurrence(
                    item["artifact_id"],
                    item["context_id"],
                    None
                    if item["exact_cut"] is None
                    else (int(item["exact_cut"]["tick"]), item["exact_cut"]["cut_id"]),
                    item["sha256"],
                    item["media_type"],
                    int(item["size_bytes"]),
                    _facts(item["common"]),
                    tuple((name, _facts(facts)) for name, facts in item["typed"].items()),
                )
                for item in value["items"]
            ),
            int(value["total"]),
            None if value["next_offset"] is None else int(value["next_offset"]),
        )

    async def analyze(self, *, index: str = "files", **selection):
        """Lazy Daft analysis of one verified bounded common or typed index."""
        if index not in ARTIFACT_INDEX_FIELDS:
            raise ValueError("Unknown artifact index")
        page = await self.occurrences(**selection)
        rows = [dict(item.facts(index)) for item in page.items if item.facts(index)]
        fields = (
            "artifact_id context_id world run tick cut_id " + ARTIFACT_INDEX_FIELDS[index]
        ).split()
        import daft

        return daft.from_pydict({name: [row[name] for row in rows] for name in fields})


@dataclass(frozen=True, slots=True)
class SyncRuntimeProgram:
    _runtime: SyncArchetypeRuntime = field(repr=False, compare=False)
    _handle: RuntimeProgram = field(repr=False)

    @property
    def name(self) -> str:
        return self._handle.name

    def publish(self, definition, **identity):
        return self._runtime._dispatch(self._handle.publish(definition, **identity))

    def resolve(self):
        return self._runtime._dispatch(self._handle.resolve())


@dataclass(frozen=True, slots=True)
class SyncRuntimeCut:
    _runtime: SyncArchetypeRuntime = field(repr=False, compare=False)
    _handle: RuntimeCut = field(repr=False)

    @property
    def world(self):
        return self._handle.world

    @property
    def run(self):
        return self._handle.run

    @property
    def tick(self):
        return self._handle.tick

    @property
    def cut_id(self):
        return self._handle.cut_id

    @property
    def parent(self):
        return self._handle.parent

    def read(self, component, **bounds):
        return self._runtime._dispatch(self._handle.read(component, **bounds))

    def analyze(self, component, **bounds):
        return self._runtime._dispatch(self._handle.analyze(component, **bounds))


@dataclass(frozen=True, slots=True)
class SyncRuntimeWorld:
    _runtime: SyncArchetypeRuntime = field(repr=False, compare=False)
    _handle: RuntimeWorld = field(repr=False)

    @property
    def name(self):
        return self._handle.name

    @property
    def world(self):
        return self._handle.world

    @property
    def run(self):
        return self._handle.run

    def create(self, program, **identity):
        return self._runtime._dispatch(self._handle.create(program, **identity))

    def status(self):
        return self._runtime._dispatch(self._handle.status())

    def start(self):
        return self._runtime._dispatch(self._handle.start())

    def stop(self):
        return self._runtime._dispatch(self._handle.stop())

    def admit(self, changes, **identity):
        return self._runtime._dispatch(self._handle.admit(changes, **identity))

    def admission_status(self, generation, admission_key):
        return self._runtime._dispatch(self._handle.admission_status(generation, admission_key))

    def publish(self, boundary):
        return SyncRuntimeCut(
            self._runtime, self._runtime._dispatch(self._handle.publish(boundary))
        )

    def reconcile(self, boundary, **identity):
        return SyncRuntimeCut(
            self._runtime, self._runtime._dispatch(self._handle.reconcile(boundary, **identity))
        )

    def confirm(self, boundary, cut):
        return self._runtime._dispatch(self._handle.confirm(boundary, cut._handle))

    def history(self, **bounds):
        page = self._runtime._dispatch(self._handle.history(**bounds))
        return CutPage(
            tuple(SyncRuntimeCut(self._runtime, c) for c in page.cuts), page.total, page.next_offset
        )

    def resume(self, cut, **identity):
        return self._runtime._dispatch(self._handle.resume(cut._handle, **identity))

    def fork(self, source, cut, **identity):
        return self._runtime._dispatch(self._handle.fork(source._handle, cut._handle, **identity))

    def artifacts(self, name):
        return SyncRuntimeArtifacts(self._runtime, self._handle.artifacts(name))

    def shutdown(self):
        return self._runtime._dispatch(self._handle.shutdown())


@dataclass(frozen=True, slots=True)
class SyncRuntimeArtifacts:
    _runtime: SyncArchetypeRuntime = field(repr=False, compare=False)
    _handle: RuntimeArtifacts = field(repr=False)

    @property
    def name(self):
        return self._handle.name

    def publish(self):
        return self._runtime._dispatch(self._handle.publish())

    def context(self):
        return self._runtime._dispatch(self._handle.context())

    def upload(self, content, *, logical_path, artifact_id, cut=None):
        return self._runtime._dispatch(
            self._handle.upload(
                content,
                logical_path=logical_path,
                artifact_id=artifact_id,
                cut=None if cut is None else cut._handle,
            )
        )

    def prepare_files(self, sources, *, cut=None):
        return self._runtime._dispatch(
            self._handle.prepare_files(sources, cut=None if cut is None else cut._handle)
        )

    def publish_files(self, prepared):
        return self._runtime._dispatch(self._handle.publish_files(prepared))

    def occurrences(self, *, cut=None, **selection):
        return self._runtime._dispatch(
            self._handle.occurrences(cut=None if cut is None else cut._handle, **selection)
        )

    def analyze(self, *, cut=None, **selection):
        return self._runtime._dispatch(
            self._handle.analyze(cut=None if cut is None else cut._handle, **selection)
        )
