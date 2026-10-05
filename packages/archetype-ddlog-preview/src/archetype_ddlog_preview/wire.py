"""Version 1 values shared by future transports. All 64-bit integers are strings."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 16 * 1024
MAX_CHANGES = 256
MAX_CELLS = 64
MAX_STRING_BYTES = 4096


def fields(value: Any, names: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != set(names.split()):
        raise ValueError("Unexpected object fields")
    return value


def identifier(value: Any) -> str:
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.:-]{0,127}", value):
        raise ValueError("Invalid identifier")
    return value


def digest(value: Any) -> str:
    if type(value) is not str or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Invalid digest")
    return value


def optional_digest(value: Any) -> str | None:
    return None if value is None else digest(value)


def decimal(value: Any, *, signed: bool = False) -> int:
    # Strings prevent a JavaScript parser from rounding before validation.
    pattern = r"(?:0|[1-9][0-9]*|-[1-9][0-9]*)" if signed else r"(?:0|[1-9][0-9]*)"
    if type(value) is not str or len(value) > 20 or not re.fullmatch(pattern, value):
        raise ValueError("Expected canonical decimal string")
    result = int(value)
    lower, upper = (-(2**63), 2**63) if signed else (0, 2**64)
    if not lower <= result < upper:
        raise ValueError("Integer outside exact range")
    return result


def unsigned(value: Any) -> str:
    if type(value) is not int or not 0 <= value < 2**64:
        raise ValueError("Invalid native unsigned integer")
    return str(value)


def string_cell(value: Any) -> str:
    if (
        type(value) is not str
        or len(value.encode("utf-8")) > MAX_STRING_BYTES
        or any(unicodedata.category(c) == "Cc" for c in value)
    ):
        raise ValueError("Invalid string cell")
    return value


@dataclass(frozen=True, slots=True)
class Cell:
    kind: Literal["int64", "string"]
    value: int | str

    @classmethod
    def decode(cls, raw: Any) -> Cell:
        if type(raw) is not dict or len(raw) != 1:
            raise ValueError("Expected one tagged cell")
        if "int64" in raw:
            return cls("int64", decimal(raw["int64"], signed=True))
        if "string" in raw:
            return cls("string", string_cell(raw["string"]))
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


type Operation = (
    Status | Start | Stop | AdmissionStatus | Admit | Publish | Reconcile | Confirm | Restore
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
}


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
        simple = {"status": Status, "start": Start, "stop": Stop}
        if type(name) is not str:
            raise ValueError("Invalid operation")
        operation: Operation
        if name in simple:
            fields(args, "")
            operation = simple[name]()
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
        else:
            raise ValueError("Unsupported operation")
        return cls(resource, operation)


def response(value: dict[str, Any]) -> bytes:
    encoded = json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()
    if len(encoded) > MAX_RESPONSE_BYTES:
        raise ValueError("Response exceeds byte limit")
    return encoded
