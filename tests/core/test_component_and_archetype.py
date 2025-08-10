import pyarrow as pa
import pytest

from archetype.core.component import Component
from archetype.core.archetype import Archetype


class Diverse(Component):
    an_int: int
    a_float: float
    a_str: str
    a_bool: bool
    a_list: list[int]
    a_bytes: bytes

class Nested(Component):
    label: str
    diverse: Diverse


class Mixed(Component):
    count: int
    ratio: float
    flag: bool


def test_component_prefix_and_row_dict():
    c = Mixed(count=3, ratio=0.5, flag=True)
    row = c.to_row_dict()
    assert row == {
        "mixed__count": 3,
        "mixed__ratio": 0.5,
        "mixed__flag": True,
    }


def test_component_prefixed_schema_names():
    schema = Mixed.get_prefixed_schema()
    assert set(schema.names) == {"mixed__count", "mixed__ratio", "mixed__flag"}


def test_archetype_schema_unifies_components():
    sig = Archetype.sig_from_components([
        Nested(label="a", diverse=Diverse(an_int=1, a_float=0.1, a_str="x", a_bool=True, a_list=[1, 2], a_bytes=b"b")),
        Mixed(count=1, ratio=1.0, flag=False),
    ])
    schema = Archetype.get_archetype_schema(sig)
    # base + prefixed
    required = {"world_id", "run_id", "entity_id", "tick", "is_active",
                "nested__label", "nested__diverse", "mixed__count", "mixed__ratio", "mixed__flag"}
    assert required.issubset(set(schema.names))


def test_archetype_name_is_stable_and_hashes_schema():
    sig1 = Archetype.sig_from_components([Nested(label="a", diverse=Diverse(an_int=0, a_float=0.0, a_str="", a_bool=False, a_list=[], a_bytes=b"")), Mixed(count=0, ratio=0.0, flag=False)])
    sig2 = Archetype.sig_from_components([Mixed(count=0, ratio=0.0, flag=False), Nested(label="a", diverse=Diverse(an_int=0, a_float=0.0, a_str="", a_bool=False, a_list=[], a_bytes=b""))])
    name1 = Archetype.get_name(sig1)
    name2 = Archetype.get_name(sig2)
    assert name1 == name2
    assert "_s" in name1  # has schema hash suffix


def test_to_row_dict_includes_base_fields_and_prefixed_components():
    row = Archetype.to_row_dict(
        entity_id=42,
        tick=7,
        components=[Nested(label="x", diverse=Diverse(an_int=2, a_float=3.0, a_str="s", a_bool=True, a_list=[1, 2, 3], a_bytes=b"z")), Mixed(count=9, ratio=1.5, flag=True)],
        world_id="w",
        run_id="r",
    )
    assert row["world_id"] == "w"
    assert row["run_id"] == "r"
    assert row["entity_id"] == 42
    assert row["tick"] == 7
    assert row["is_active"] is True
    for key in ["nested__label", "nested__diverse", "mixed__count", "mixed__ratio", "mixed__flag"]:
        assert key in row
    assert isinstance(row["nested__diverse"], dict)

