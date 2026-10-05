"""Bounded immutable program declarations and exact protected references."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .values import digest, fields, identifier


def _sequence(value: Any, limit: int, *, nonempty: bool = False) -> list[Any]:
    if type(value) is not list or not int(nonempty) <= len(value) <= limit:
        raise ValueError("Invalid declaration count")
    return value


def _unique(names: tuple[str, ...]) -> None:
    if len(set(names)) != len(names):
        raise ValueError("Duplicate declaration name")


def _tuple(value: Any, kind: type, limit: int, minimum: int = 0) -> None:
    if (
        type(value) is not tuple
        or not minimum <= len(value) <= limit
        or any(type(item) is not kind for item in value)
    ):
        raise ValueError("Expected bounded immutable declarations")


def _types(value: Any) -> tuple[str, ...]:
    result = tuple(_sequence(value, 64, nonempty=True))
    if any(type(t) is not str or t not in ("int64", "string", "bool", "float64") for t in result):
        raise ValueError("Invalid program field type")
    return result


def native_types(types: tuple[str, ...]) -> list[str]:
    return [{"int64": "int", "float64": "double"}.get(t, t) for t in types]


@dataclass(frozen=True, slots=True)
class ProgramReference:
    resource: str
    processor_id: str
    version: str

    def __post_init__(self) -> None:
        identifier(self.resource)
        identifier(self.processor_id)
        if type(self.version) is not str or not self.version.startswith("sha256:"):
            raise ValueError("Expected exact immutable program version")
        digest(self.version[7:])

    @classmethod
    def decode(cls, value: Any) -> ProgramReference:
        row = fields(value, "resource processor_id version")
        return cls(row["resource"], row["processor_id"], row["version"])

    def pin(self) -> dict[str, str]:
        return {"processor_id": self.processor_id, "version": self.version}

    def native(self) -> dict[str, Any]:
        return {"resource": self.resource, "processor": self.pin()}


@dataclass(frozen=True, slots=True)
class Relation:
    name: str
    input: bool
    types: tuple[str, ...]

    def __post_init__(self) -> None:
        identifier(self.name)
        if type(self.input) is not bool:
            raise ValueError("Expected exact input flag")
        _tuple(self.types, str, 64, 1)
        _types(list(self.types))

    @classmethod
    def decode(cls, value: Any) -> Relation:
        row = fields(value, "name input fields")
        if type(row["input"]) is not bool:
            raise ValueError("Expected exact input flag")
        return cls(identifier(row["name"]), row["input"], _types(row["fields"]))


@dataclass(frozen=True, slots=True)
class LeafProgram:
    rules: str
    schemas: tuple[Relation, ...]
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            type(self.rules) is not str
            or not 1 <= len(self.rules.encode("utf-8")) <= 32768
            or "\0" in self.rules
        ):
            raise ValueError("Invalid program source")
        _tuple(self.schemas, Relation, 128, 1)
        _tuple(self.inputs, str, 64)
        _tuple(self.outputs, str, 64, 1)
        for names in (tuple(s.name for s in self.schemas), self.inputs, self.outputs):
            for name in names:
                identifier(name)
            _unique(names)
        schemas = {s.name: s for s in self.schemas}
        if set(self.inputs) != {s.name for s in self.schemas if s.input} or any(
            n not in schemas or schemas[n].input for n in self.outputs
        ):
            raise ValueError("Program interface differs from declarations")

    @classmethod
    def decode(cls, value: Any) -> LeafProgram:
        row = fields(value, "rules schemas inputs outputs")
        schemas = tuple(Relation.decode(v) for v in _sequence(row["schemas"], 128, nonempty=True))
        inputs = tuple(identifier(v) for v in _sequence(row["inputs"], 64))
        outputs = tuple(identifier(v) for v in _sequence(row["outputs"], 64, nonempty=True))
        return cls(row["rules"], schemas, inputs, outputs)

    def native(self) -> dict[str, Any]:
        return {
            "rules": self.rules,
            "schemas": {
                s.name: {"input": s.input, "fields": native_types(s.types)} for s in self.schemas
            },
            "interface": {"inputs": list(self.inputs), "outputs": list(self.outputs)},
        }


@dataclass(frozen=True, slots=True)
class Endpoint:
    node: str
    relation: str

    def __post_init__(self) -> None:
        identifier(self.node)
        identifier(self.relation)

    @classmethod
    def decode(cls, value: Any) -> Endpoint:
        row = fields(value, "node relation")
        return cls(identifier(row["node"]), identifier(row["relation"]))

    def native(self) -> dict[str, str]:
        return {"node": self.node, "relation": self.relation}


@dataclass(frozen=True, slots=True)
class ProgramNode:
    name: str
    program: ProgramReference

    def __post_init__(self) -> None:
        identifier(self.name)
        if type(self.program) is not ProgramReference:
            raise ValueError("Expected exact program reference")


@dataclass(frozen=True, slots=True)
class InputPort:
    name: str
    types: tuple[str, ...]
    targets: tuple[Endpoint, ...]

    def __post_init__(self) -> None:
        identifier(self.name)
        _tuple(self.types, str, 64, 1)
        _types(list(self.types))
        _tuple(self.targets, Endpoint, 64, 1)


@dataclass(frozen=True, slots=True)
class Connection:
    source: Endpoint
    target: Endpoint

    def __post_init__(self) -> None:
        if type(self.source) is not Endpoint or type(self.target) is not Endpoint:
            raise ValueError("Expected connection endpoints")


@dataclass(frozen=True, slots=True)
class OutputPort:
    name: str
    source: Endpoint

    def __post_init__(self) -> None:
        identifier(self.name)
        if type(self.source) is not Endpoint:
            raise ValueError("Expected output endpoint")


@dataclass(frozen=True, slots=True)
class Composition:
    nodes: tuple[ProgramNode, ...]
    inputs: tuple[InputPort, ...]
    bindings: tuple[Connection, ...]
    outputs: tuple[OutputPort, ...]

    def __post_init__(self) -> None:
        _tuple(self.nodes, ProgramNode, 64, 1)
        _tuple(self.inputs, InputPort, 64)
        _tuple(self.bindings, Connection, 256)
        _tuple(self.outputs, OutputPort, 64, 1)
        for names in (
            tuple(n.name for n in self.nodes),
            tuple(p.name for p in self.inputs),
            tuple(p.name for p in self.outputs),
        ):
            _unique(names)
        known = {n.name for n in self.nodes}
        endpoints = (
            [t for p in self.inputs for t in p.targets]
            + [e for b in self.bindings for e in (b.source, b.target)]
            + [p.source for p in self.outputs]
        )
        if any(e.node not in known for e in endpoints):
            raise ValueError("Unknown composition node")

    @classmethod
    def decode(cls, value: Any) -> Composition:
        row = fields(value, "nodes inputs bindings outputs")
        nodes = []
        for raw in _sequence(row["nodes"], 64, nonempty=True):
            node = fields(raw, "name program")
            nodes.append(
                ProgramNode(identifier(node["name"]), ProgramReference.decode(node["program"]))
            )
        inputs = []
        for raw in _sequence(row["inputs"], 64):
            port = fields(raw, "name fields targets")
            inputs.append(
                InputPort(
                    identifier(port["name"]),
                    _types(port["fields"]),
                    tuple(
                        Endpoint.decode(t) for t in _sequence(port["targets"], 64, nonempty=True)
                    ),
                )
            )
        bindings = []
        for raw in _sequence(row["bindings"], 256):
            edge = fields(raw, "from to")
            bindings.append(Connection(Endpoint.decode(edge["from"]), Endpoint.decode(edge["to"])))
        outputs = []
        for raw in _sequence(row["outputs"], 64, nonempty=True):
            port = fields(raw, "name source")
            outputs.append(OutputPort(identifier(port["name"]), Endpoint.decode(port["source"])))
        for names in (
            tuple(n.name for n in nodes),
            tuple(p.name for p in inputs),
            tuple(p.name for p in outputs),
        ):
            _unique(names)
        known = {n.name for n in nodes}
        endpoints = (
            [t for p in inputs for t in p.targets]
            + [e for b in bindings for e in (b.source, b.target)]
            + [p.source for p in outputs]
        )
        if any(e.node not in known for e in endpoints):
            raise ValueError("Unknown composition node")
        return cls(tuple(nodes), tuple(inputs), tuple(bindings), tuple(outputs))

    def references(self) -> tuple[ProgramReference, ...]:
        return tuple(node.program for node in self.nodes)

    def native(self) -> dict[str, Any]:
        return {
            "composition": {
                "nodes": {n.name: n.program.pin() for n in self.nodes},
                "inputs": {
                    p.name: {
                        "fields": native_types(p.types),
                        "targets": [t.native() for t in p.targets],
                    }
                    for p in self.inputs
                },
                "bindings": [
                    {"from": b.source.native(), "to": b.target.native()} for b in self.bindings
                ],
                "outputs": {p.name: p.source.native() for p in self.outputs},
            }
        }
