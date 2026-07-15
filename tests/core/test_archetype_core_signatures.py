import pytest

from archetype.core.archetype import Archetype
from archetype.core.component import Component


class A(Component):
    x: int


class B(Component):
    y: int


class PrefixCollisionA(Component):
    value: int = 0


class prefixcollisiona(Component):  # noqa: N801 — intentional name collision
    value: int = 0


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
    """Core base schema and partition keys should remain stable over time (API contract).

    Amended by issue #273 (approved spec amendment): the storage base schema
    carries the commit identity; the v0.2 shape is frozen as LEGACY_BASE_SCHEMA
    and names the read-only legacy tables.
    """
    assert set(Archetype.BASE_SCHEMA.names) == {
        "world_id",
        "run_id",
        "entity_id",
        "tick",
        "is_active",
        "commit_token",
        "writer_epoch",
    }
    assert set(Archetype.LEGACY_BASE_SCHEMA.names) == {
        "world_id",
        "run_id",
        "entity_id",
        "tick",
        "is_active",
    }
    assert Archetype.COMMIT_FIELD_NAMES == ("commit_token", "writer_epoch")
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


def test_get_archetype_schema_raises_on_prefix_collision():
    """Two distinct components with the same lowercased class name must not be
    silently merged into a single set of fields — the schema builder must raise
    a clear ValueError instead of corrupting the row layout."""
    assert PrefixCollisionA.get_prefix() == prefixcollisiona.get_prefix()
    with pytest.raises(ValueError, match="prefix collision"):
        Archetype.get_archetype_schema((PrefixCollisionA, prefixcollisiona))


def test_to_row_dict_raises_on_prefix_collision_between_distinct_classes():
    """Combining two same-prefix components on one entity must raise instead of
    letting the second silently overwrite the first under the shared key."""
    with pytest.raises(ValueError, match="prefix collision"):
        Archetype.to_row_dict(
            entity_id=1,
            tick=0,
            components=[PrefixCollisionA(value=1), prefixcollisiona(value=99)],
            world_id="w",
            run_id="r",
        )


def test_to_row_dict_allows_duplicate_instances_of_same_class():
    """The collision check must only fire for *distinct* classes that share a
    prefix; passing two instances of the same class is the existing
    "last-write-wins" dedupe path and must continue to work."""
    row = Archetype.to_row_dict(
        entity_id=1,
        tick=0,
        components=[PrefixCollisionA(value=1), PrefixCollisionA(value=2)],
        world_id="w",
        run_id="r",
    )
    assert row["prefixcollisiona__value"] == 2


def test_get_archetype_schema_allows_repeated_same_type_in_sig():
    """A signature that lists the same component class more than once must not
    trigger the collision check — only distinct classes with colliding prefixes
    should fail."""
    schema = Archetype.get_archetype_schema((PrefixCollisionA,))
    assert "prefixcollisiona__value" in set(schema.names)


def test_archetype_instance_populates_sig_name_and_schema():
    """The Archetype constructor is used at the query layer
    (``core/sync/querier.py`` and ``core/aio/async_querier.py``) to bundle a
    component list into its signature, archetype name, and unified PyArrow
    schema. Each attribute must equal what the corresponding staticmethod
    returns — drift between ``__init__`` and the static API would silently
    break the queriers' archetype lookups."""
    components = [B(y=2), A(x=1)]
    archetype = Archetype(components)

    expected_sig = Archetype.sig_from_components(components)
    assert archetype.components is components
    assert archetype.sig == expected_sig
    assert archetype.name == Archetype.get_name(expected_sig)
    assert archetype.schema == Archetype.get_archetype_schema(expected_sig)
