from archetype.core.component import Component


class Mixed(Component):
    count: int
    ratio: float
    flag: bool


def test_component_prefix_and_to_row_dict():
    """Component prefixing is lowercase class name with '__' and to_row_dict prefixes all fields."""
    c = Mixed(count=3, ratio=0.5, flag=True)
    assert c.get_prefix() == "mixed__"
    row = c.to_row_dict()
    assert row == {"mixed__count": 3, "mixed__ratio": 0.5, "mixed__flag": True}


def test_component_prefixed_schema_fields_exist():
    """Prefixed schema should contain component fields with proper prefix applied."""
    schema = Mixed.get_prefixed_schema()
    assert set(schema.names) == {"mixed__count", "mixed__ratio", "mixed__flag"}


class DemoUnique(Component):
    a: int


def test_get_type_by_name_and_from_dict():
    """Dynamic type lookup and dict-based construction should work for LanceModel-based components."""
    # get_type_by_name searches subclasses of Component globally;
    # ensure we reference the class name from this module (avoid name collisions across tests)
    T = Component.get_type_by_name("DemoUnique")
    assert T.__name__ == DemoUnique.__name__
    inst = Component.from_dict({"type": DemoUnique.__name__, "a": 5})
    assert isinstance(inst, DemoUnique)
    assert inst.a == 5


def test_get_type_by_name_raises_for_missing():
    """Looking up an unknown Component subclass by name should raise a clear ValueError."""
    import pytest

    with pytest.raises(ValueError):
        Component.get_type_by_name("DoesNotExist")


class _Intermediate(Component):
    shared: int = 0


class DeepNested(_Intermediate):
    extra: str = ""


def test_get_type_by_name_walks_subclass_tree():
    """get_type_by_name should find components defined as subclasses of intermediate bases.

    Regression test for #90: ``__subclasses__()`` only returns direct subclasses,
    so a naive lookup silently misses deeply-nested component classes.
    """
    T = Component.get_type_by_name("DeepNested")
    assert T is DeepNested
    inst = Component.from_dict({"type": "DeepNested", "shared": 1, "extra": "x"})
    assert isinstance(inst, DeepNested)
    assert inst.shared == 1 and inst.extra == "x"


def test_from_dict_without_type_key_raises_clearly():
    """Component.from_dict on the base class must refuse to silently lose type info.

    Regression test for #90: previously, missing 'type' produced a generic
    ``Component`` instance with no fields, so SPAWN commands submitted via
    ``CommandService`` landed in the wrong archetype.
    """
    import pytest

    with pytest.raises(ValueError, match="'type' key"):
        Component.from_dict({"a": 5})


def test_to_payload_round_trips_through_from_dict():
    """to_payload() produces a dict that from_dict() reconstructs into the same type."""
    original = DemoUnique(a=7)
    payload = original.to_payload()
    assert payload["type"] == "DemoUnique"
    restored = Component.from_dict(payload)
    assert isinstance(restored, DemoUnique)
    assert restored.a == 7


def test_from_dict_does_not_mutate_input():
    """from_dict does not mutate the caller's dict.

    Previously ``data.pop("type", None)`` removed the key in-place, so a
    second call on the same dict returned a bare ``Component`` instead of
    the intended subclass. This broke config-driven templates, retry loops,
    and JSON round-trips. See
    ``docs/reports/2026-04-11-bug-from-dict-mutates-input.md``.
    """
    template = {"type": "DemoUnique", "a": 1}
    snapshot = dict(template)

    first = Component.from_dict(template)
    # Input dict must be untouched after the call.
    assert template == snapshot
    assert isinstance(first, DemoUnique)
    assert first.a == 1

    # A second call on the same dict must still yield the concrete subclass.
    second = Component.from_dict(template)
    assert isinstance(second, DemoUnique), (
        f"from_dict mutated its input: second call returned "
        f"{type(second).__name__}, expected DemoUnique"
    )
    assert second.a == 1
    assert template == snapshot


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


def test_to_arrow_schema_supports_nested_and_bytes_types():
    """LanceModel.to_arrow_schema should preserve nested struct and bytes fields for complex components."""
    import pyarrow as pa

    diverse_schema = Diverse.to_arrow_schema()
    assert isinstance(diverse_schema, pa.Schema)
    assert set(diverse_schema.names) == {
        "an_int",
        "a_float",
        "a_str",
        "a_bool",
        "a_list",
        "a_bytes",
    }

    nested_schema = Nested.to_arrow_schema()
    # Nested.diverse should be a struct field
    field = nested_schema.field("diverse")
    assert field.type.__class__.__name__.lower() == "structtype"

    # Prefixed nested schema should rename top-level fields appropriately
    pref = Nested.get_prefixed_schema()
    assert "nested__diverse" in pref.names
