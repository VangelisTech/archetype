import pyarrow as pa
from archetype.core.archetype import Archetype
from archetype.core.component import Component


class A(Component):
    x: int


class B(Component):
    y: int


def test_sig_from_components_is_sorted_by_type_name():
    """Signature creation should be order-invariant and sorted by component type names."""
    sig1 = Archetype.sig_from_components([B(y=1), A(x=1)])
    sig2 = Archetype.sig_from_components([A(x=1), B(y=1)])
    assert sig1 == sig2
    # Sorted lexicographically by class name
    assert [t.__name__ for t in sig1] == ["A", "B"]


def test_add_and_remove_components_are_set_like_and_sorted():
    """Adding/removing components behaves like set union/difference and preserves sorted order by name."""
    base = Archetype.sig_from_components([A(x=0)])
    added = Archetype.add_components(base, [B])
    assert [t.__name__ for t in added] == ["A", "B"]

    removed = Archetype.remove_components(added, [A])
    assert removed == (B,)


def test_base_schema_and_partition_keys_are_stable():
    """Core base schema and partition keys should remain stable over time (API contract)."""
    # Names only (types covered elsewhere); keep invariant stable
    assert set(Archetype.BASE_SCHEMA.names) == {
        "world_id", "run_id", "entity_id", "tick", "is_active"
    }
    assert Archetype.PARTITION_KEYS == ["world_id", "run_id", "tick"]


def test_get_archetype_schema_unifies_base_and_components():
    """Archetype schema unifies base columns with prefixed component schemas without collisions."""
    sig = Archetype.sig_from_components([A(x=1), B(y=2)])
    schema = Archetype.get_archetype_schema(sig)
    assert set(["world_id", "run_id", "entity_id", "tick", "is_active"]).issubset(set(schema.names))
    assert set(["a__x", "b__y"]).issubset(set(schema.names))


def test_get_name_is_order_invariant_and_hash_suffixed():
    """Archetype name is compact, order-invariant, and contains a schema-hash suffix for uniqueness."""
    sig1 = Archetype.sig_from_components([A(x=1), B(y=2)])
    sig2 = Archetype.sig_from_components([B(y=2), A(x=1)])
    n1 = Archetype.get_name(sig1)
    n2 = Archetype.get_name(sig2)
    assert n1 == n2
    assert n1.startswith("a_2c_s")


