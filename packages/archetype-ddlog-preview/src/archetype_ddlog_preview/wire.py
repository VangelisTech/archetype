"""Version 1 values shared by future transports. All 64-bit integers are strings."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from .programs import Composition, LeafProgram, ProgramReference
from .values import (
    MAX_CELLS as MAX_CELLS,
)
from .values import (
    MAX_STRING_BYTES as MAX_STRING_BYTES,
)
from .values import bool_cell, decode_float_bits
from .values import (
    decimal as decimal,
)
from .values import (
    digest as digest,
)
from .values import (
    fields as fields,
)
from .values import (
    identifier as identifier,
)
from .values import (
    optional_digest as optional_digest,
)
from .values import (
    string_cell as string_cell,
)
from .values import (
    unsigned as unsigned,
)

MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 16 * 1024
MAX_CHANGES = 256


@dataclass(frozen=True, slots=True)
class Cell:
    kind: Literal["int64", "string", "bool", "float64"]
    value: int | str | bool | float

    @classmethod
    def decode(cls, raw: Any) -> Cell:
        if type(raw) is not dict or len(raw) != 1:
            raise ValueError("Expected one tagged cell")
        if "int64" in raw:
            return cls("int64", decimal(raw["int64"], signed=True))
        if "string" in raw:
            return cls("string", string_cell(raw["string"]))
        if "bool" in raw:
            return cls("bool", bool_cell(raw["bool"]))
        if "float64" in raw:
            return cls("float64", decode_float_bits(raw["float64"]))
        raise ValueError("Unknown cell type")


@dataclass(frozen=True, slots=True)
class Change:
    op: str
    predicate: str
    cells: tuple[Cell, ...]

    @classmethod
    def decode(cls, raw: Any) -> Change:
        row = fields(raw, "op predicate values")
        if row["op"] not in ("insert", "delete"):
            raise ValueError("Unknown change operation")
        if type(row["values"]) is not list or not 1 <= len(row["values"]) <= MAX_CELLS:
            raise ValueError("Invalid cell count")
        return cls(row["op"], identifier(row["predicate"]), tuple(map(Cell.decode, row["values"])))

    def native(self) -> dict[str, Any]:
        return {"op": self.op, "predicate": self.predicate, "values": [c.value for c in self.cells]}


@dataclass(frozen=True, slots=True)
class Boundary:
    generation: int
    admission_key: str
    request_sha256: str

    @classmethod
    def decode(cls, raw: Any) -> Boundary:
        row = fields(raw, "generation admission_key request_sha256")
        return cls(
            decimal(row["generation"]),
            identifier(row["admission_key"]),
            digest(row["request_sha256"]),
        )

    def native(self, native_world: str) -> dict[str, Any]:
        return {
            "world_id": native_world,
            "generation": self.generation,
            "admission_key": self.admission_key,
            "request_sha256": self.request_sha256,
        }


@dataclass(frozen=True, slots=True)
class Receipt:
    world: str
    run: str
    tick: int
    cut_id: str

    @classmethod
    def decode(cls, raw: Any) -> Receipt:
        row = fields(raw, "world run tick cut_id")
        return cls(
            identifier(row["world"]),
            identifier(row["run"]),
            decimal(row["tick"]),
            digest(row["cut_id"]),
        )

    def native(self) -> dict[str, Any]:
        return {"world": self.world, "run": self.run, "tick": self.tick, "cut_id": self.cut_id}


@dataclass(frozen=True, slots=True)
class Status:
    name: ClassVar[str] = "status"


@dataclass(frozen=True, slots=True)
class Start:
    name: ClassVar[str] = "start"


@dataclass(frozen=True, slots=True)
class Stop:
    name: ClassVar[str] = "stop"


@dataclass(frozen=True, slots=True)
class Create:
    name: ClassVar[str] = "create"
    request_key: str
    label: str
    program: ProgramReference


@dataclass(frozen=True, slots=True)
class Resolve:
    name: ClassVar[str] = "resolve"


@dataclass(frozen=True, slots=True)
class ProgramCreate:
    name: ClassVar[str] = "program_create"
    request_key: str
    description: str
    definition: LeafProgram


@dataclass(frozen=True, slots=True)
class ProgramCompose:
    name: ClassVar[str] = "program_compose"
    request_key: str
    description: str
    composition: Composition


@dataclass(frozen=True, slots=True)
class ProgramResolve:
    name: ClassVar[str] = "program_resolve"


@dataclass(frozen=True, slots=True)
class ProgramDescribe:
    name: ClassVar[str] = "program_describe"


@dataclass(frozen=True, slots=True)
class AdmissionStatus:
    name: ClassVar[str] = "admission_status"
    generation: int
    admission_key: str


@dataclass(frozen=True, slots=True)
class Admit:
    name: ClassVar[str] = "admit"
    generation: int
    revision: int
    admission_key: str
    expected_head: str | None
    changes: tuple[Change, ...]


@dataclass(frozen=True, slots=True)
class Publish:
    name: ClassVar[str] = "publish"
    boundary: Boundary


@dataclass(frozen=True, slots=True)
class Reconcile:
    name: ClassVar[str] = "reconcile"
    boundary: Boundary
    tick: int
    expected_parent: str | None


@dataclass(frozen=True, slots=True)
class Confirm:
    name: ClassVar[str] = "confirm"
    boundary: Boundary
    tick: int
    expected_parent: str | None


@dataclass(frozen=True, slots=True)
class Restore:
    name: ClassVar[str] = "restore"
    receipt: Receipt
    expected_generation: int


@dataclass(frozen=True, slots=True)
class Fork:
    name: ClassVar[str] = "fork"
    source_resource: str
    receipt: Receipt
    request_key: str
    expected_generation: int


@dataclass(frozen=True, slots=True)
class PublishContext:
    name: ClassVar[str] = "publish_context"
    source_resource: str | None


@dataclass(frozen=True, slots=True)
class ReadContext:
    name: ClassVar[str] = "read_context"


@dataclass(frozen=True, slots=True)
class ContextArtifacts:
    name: ClassVar[str] = "context_artifacts"
    context_id: str
    exact_cut: tuple[int, str] | None
    all: bool
    offset: int
    limit: int

    def selection(self) -> dict[str, Any]:
        if self.all:
            return {"kind": "all"}
        return {
            "kind": "target",
            "exact_cut": None
            if self.exact_cut is None
            else {"tick": self.exact_cut[0], "cut_id": self.exact_cut[1]},
        }


type Operation = (
    Status
    | Start
    | Stop
    | AdmissionStatus
    | Admit
    | Publish
    | Reconcile
    | Confirm
    | Restore
    | Fork
    | PublishContext
    | ReadContext
    | ContextArtifacts
    | Create
    | Resolve
    | ProgramCreate
    | ProgramCompose
    | ProgramResolve
    | ProgramDescribe
)

CAPABILITIES: dict[type[Operation], str] = {
    Status: "simulation:read",
    AdmissionStatus: "simulation:read",
    Start: "simulation:control",
    Stop: "simulation:control",
    Admit: "simulation:submit",
    Publish: "simulation:publish",
    Reconcile: "simulation:publish",
    Confirm: "simulation:confirm",
    Restore: "simulation:restore",
    Fork: "simulation:fork",
    PublishContext: "artifacts:publish",
    ReadContext: "artifacts:read",
    ContextArtifacts: "artifacts:read",
    Create: "simulation:create",
    Resolve: "simulation:read",
    ProgramCreate: "programs:create",
    ProgramCompose: "programs:create",
    ProgramResolve: "programs:read",
    ProgramDescribe: "programs:read",
}


def requirements(request: Request) -> tuple[tuple[str, str], ...]:
    """Complete grant set derived only from the closed request, before lookup."""
    op = request.operation
    required = [(request.resource, CAPABILITIES[type(op)])]
    if isinstance(op, (Fork, PublishContext)) and op.source_resource is not None:
        required.append((op.source_resource, CAPABILITIES[type(op)]))
    if isinstance(op, Create):
        required.append((op.program.resource, "programs:read"))
    if isinstance(op, ProgramCompose):
        required.extend((ref.resource, "programs:read") for ref in op.composition.references())
    return tuple(required)


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate key")
        result[key] = value
    return result


def _no_float(_: str) -> Any:
    raise ValueError("Floating point input is not supported")


@dataclass(frozen=True, slots=True)
class Request:
    resource: str
    operation: Operation

    @classmethod
    def decode(cls, raw: bytes) -> Request:
        if type(raw) is not bytes or not 0 < len(raw) <= MAX_REQUEST_BYTES:
            raise ValueError("Request exceeds byte limit")
        obj = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_object,
            parse_float=_no_float,
            parse_constant=_no_float,
        )
        row = fields(obj, "version operation resource arguments")
        if type(row["version"]) is not int or row["version"] != 1:
            raise ValueError("Unknown contract version")
        resource, name, args = identifier(row["resource"]), row["operation"], row["arguments"]
        simple = {
            "status": Status,
            "start": Start,
            "stop": Stop,
            "resolve": Resolve,
            "program_resolve": ProgramResolve,
            "program_describe": ProgramDescribe,
        }
        if type(name) is not str:
            raise ValueError("Invalid operation")
        operation: Operation
        if name in simple:
            fields(args, "")
            operation = simple[name]()
        elif name == "create":
            fields(args, "request_key label program")
            operation = Create(
                identifier(args["request_key"]),
                string_cell(args["label"]),
                ProgramReference.decode(args["program"]),
            )
        elif name == "program_create":
            fields(args, "request_key description definition")
            operation = ProgramCreate(
                identifier(args["request_key"]),
                string_cell(args["description"]),
                LeafProgram.decode(args["definition"]),
            )
        elif name == "program_compose":
            fields(args, "request_key description composition")
            operation = ProgramCompose(
                identifier(args["request_key"]),
                string_cell(args["description"]),
                Composition.decode(args["composition"]),
            )
        elif name == "admission_status":
            fields(args, "generation admission_key")
            operation = AdmissionStatus(
                decimal(args["generation"]), identifier(args["admission_key"])
            )
        elif name == "admit":
            fields(args, "generation revision admission_key expected_head changes")
            if type(args["changes"]) is not list or not 1 <= len(args["changes"]) <= MAX_CHANGES:
                raise ValueError("Invalid change count")
            operation = Admit(
                decimal(args["generation"]),
                decimal(args["revision"]),
                identifier(args["admission_key"]),
                optional_digest(args["expected_head"]),
                tuple(map(Change.decode, args["changes"])),
            )
        elif name == "publish":
            fields(args, "boundary")
            operation = Publish(Boundary.decode(args["boundary"]))
        elif name in ("reconcile", "confirm"):
            fields(args, "boundary tick expected_parent")
            constructor = Reconcile if name == "reconcile" else Confirm
            operation = constructor(
                Boundary.decode(args["boundary"]),
                decimal(args["tick"]),
                optional_digest(args["expected_parent"]),
            )
        elif name == "restore":
            fields(args, "receipt expected_generation")
            operation = Restore(
                Receipt.decode(args["receipt"]), decimal(args["expected_generation"])
            )
        elif name == "fork":
            fields(args, "source_resource receipt request_key expected_generation")
            operation = Fork(
                identifier(args["source_resource"]),
                Receipt.decode(args["receipt"]),
                identifier(args["request_key"]),
                decimal(args["expected_generation"]),
            )
        elif name == "publish_context":
            fields(args, "source_resource")
            operation = PublishContext(
                None if args["source_resource"] is None else identifier(args["source_resource"])
            )
        elif name == "read_context":
            fields(args, "")
            operation = ReadContext()
        elif name == "context_artifacts":
            fields(args, "context_id exact_cut all offset limit")
            exact = None
            if args["exact_cut"] is not None:
                cut = fields(args["exact_cut"], "tick cut_id")
                tick = decimal(cut["tick"])
                if not 0 < tick < 2**63:
                    raise ValueError("Invalid exact cut tick")
                exact = (tick, digest(cut["cut_id"]))
            offset, limit = decimal(args["offset"]), decimal(args["limit"])
            if (
                type(args["all"]) is not bool
                or (args["all"] and exact is not None)
                or not 1 <= limit <= 32
            ):
                raise ValueError("Invalid artifact selection")
            operation = ContextArtifacts(
                digest(args["context_id"]), exact, args["all"], offset, limit
            )
        else:
            raise ValueError("Unsupported operation")
        return cls(resource, operation)


def response(value: dict[str, Any]) -> bytes:
    encoded = json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()
    if len(encoded) > MAX_RESPONSE_BYTES:
        raise ValueError("Response exceeds byte limit")
    return encoded
