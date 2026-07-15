# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from daft import DataFrame

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.interfaces import ArchetypeSignature, iAsyncQueryManager, iAsyncStore

logger = logging.getLogger(__name__)


class UnknownSignatureError(LookupError):
    """Raised when a query targets an archetype signature that has never been
    registered in this world (distinct from 'no rows at this tick').

    A signature that exists but has zero rows at tick N produces an empty
    DataFrame; a signature that was never spawned raises this error.
    """


class AsyncQueryManager(iAsyncQueryManager):
    def __init__(self, store: iAsyncStore):
        self._store = store

    async def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types for provided world_id and run_id.
        """
        # Canonicalize so unsorted caller-supplied tuples resolve to the same table.
        sig = _canonicalize(sig)
        return await self._store.get_archetype_df(
            sig=sig, world_id=world_id, run_id=run_id, active_only=True
        )

    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type["Component"]] | None = None,
        run_id: str | None = None,
        commit_tokens: list[str] | None = None,
        **kwargs,  # absorbs world-internal keys (e.g. run_config, _world_validated)
    ) -> DataFrame:
        """Query active entities for the provided archetype signature.

        The signature is canonicalized (sorted by class name) before table
        resolution, so callers may supply types in any order and will always
        find the same table.

        Raises ``UnknownSignatureError`` when the canonical signature has never
        been registered in the durable store — distinct from 'the signature
        exists but has zero rows at this tick', which returns an empty DataFrame.

        ``_world_validated=True`` may be passed by the world's ``query_archetype``
        facade after it has already verified the sig against live and store sigs.
        When set, the querier skips its own redundant check so that sigs that are
        live (in spawn cache) but not yet committed do not cause a false positive.
        """
        if run_id is None:
            # Reads MUST be scoped by world_id and run_id (spec §137). A missing
            # run_id would otherwise stringify to "None" and silently match nothing.
            raise ValueError("query_archetype requires run_id to scope the read")

        # Canonicalize so an unsorted tuple resolves to the same underlying table
        # as its sorted counterpart.  Internals always produce sorted sigs; this
        # guard makes the public API lenient while keeping the storage layer pure.
        sig = _canonicalize(sig)

        # Existence check: the sig must appear in the store's committed-signature
        # registry.  An unknown sig must raise UnknownSignatureError distinctly
        # rather than silently returning an empty create-on-read table.
        #
        # We use list_committed_signatures() so that auto-created tables from
        # concurrent get_archetype_df calls in the same tick batch do not
        # pollute the check.  Only sigs durably committed via append() are
        # considered "known" here.  When the committed registry is empty (brand-
        # new world, nothing flushed yet, or in-memory store without persistence),
        # the check is skipped — an empty registry cannot distinguish "not yet
        # committed" from "truly unknown", so we let the store return an empty
        # frame naturally.
        #
        # Skip the check when the world-level guard has already validated the sig
        # (``_world_validated=True``).  The world guard also considers live/pending
        # sigs in the spawn cache, which are not yet reflected in
        # list_committed_signatures(); without this bypass, newly-spawned sigs
        # that haven't been flushed would produce false-positive errors.
        world_validated: bool = kwargs.pop("_world_validated", False)
        if not world_validated and hasattr(self._store, "list_committed_signatures"):
            committed_sigs = {
                _canonicalize(s) for s in await self._store.list_committed_signatures()
            }
            if committed_sigs and sig not in committed_sigs:
                component_names = ", ".join(t.__name__ for t in sig)
                raise UnknownSignatureError(
                    f"Archetype signature ({component_names}) has never been registered in this world. "
                    "No entity carrying exactly these component types has been spawned. "
                    "If you want to query entities that CONTAIN these components (possibly alongside others), "
                    "use world.query() / query_components() instead of query_archetype()."
                )

        df = await self._store.get_archetype_df(
            sig=sig,
            world_id=world_id,
            run_id=run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            active_only=True,
            commit_tokens=commit_tokens,
        )

        if components:
            df = df.select(*Archetype.projection_columns(components))

        return df

    async def query_components(
        self,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        commit_tokens: list[str] | None = None,
    ) -> DataFrame:
        """Query all entities that contain the requested component types.

        Discovers matching archetype signatures, queries each, projects to
        the requested component columns, and unions the results.

        If a requested component type does not exist in ANY registered
        archetype, a ``KeyError`` is raised naming the missing component so
        callers can detect the ManipProprio-vs-ManipAction style confusion
        (querying a component that belongs to a different archetype).
        """
        import daft
        import pyarrow as pa

        required = set(components)

        # Build the output schema from the requested components. Component
        # projections exclude commit-identity columns (they are storage
        # metadata; raw query_archetype reads still expose them).
        output_sig = tuple(sorted(components, key=lambda t: t.__name__))
        proj_cols = Archetype.projection_columns(list(output_sig))
        full_schema = Archetype.get_archetype_schema(output_sig)
        schema = pa.schema([full_schema.field(name) for name in proj_cols])

        # Find all sigs that contain the required types
        all_sigs = await self.list_signatures()
        matching = [sig for sig in all_sigs if required.issubset(set(sig))]

        # Ergonomic check: when no archetype satisfies the full component set,
        # raise with a message that names the problematic component(s) and hints
        # at where each one actually lives.  This catches two related traps:
        #   (a) A component was never spawned in this world at all.
        #   (b) Each component exists on a separate archetype but never together
        #       (the ManipProprio-vs-ManipAction trap).
        if not matching and all_sigs:
            all_component_types: set[type[Component]] = set()
            for s in all_sigs:
                all_component_types.update(s)

            # Build per-component diagnostic hints.
            hints: list[str] = []
            culprit_names: list[str] = []
            for c in components:
                providers = [s for s in all_sigs if c in s]
                if not providers:
                    # Case (a): component has never been spawned at all.
                    hints.append(f"{c.__name__} has never been spawned in this world")
                    culprit_names.append(c.__name__)
                else:
                    # Case (b): component exists but not together with all others.
                    provider_names = ", ".join(
                        "({})".format(", ".join(t.__name__ for t in s)) for s in providers
                    )
                    hints.append(
                        f"{c.__name__} is provided by archetype(s) {provider_names} "
                        "but not together with all other requested components"
                    )
                    culprit_names.append(c.__name__)

            # Always raise when we have a clear mismatch so the caller gets
            # the named-component error rather than an empty result.
            culprits = ", ".join(culprit_names)
            raise KeyError(f"No archetype contains all of: {culprits}. " + " | ".join(hints))

        # Start with empty DataFrame of the right schema
        result = daft.from_arrow(pa.Table.from_batches([], schema=schema))

        for sig in matching:
            df = await self._store.get_archetype_df(
                sig=sig,
                world_id=world_id,
                run_id=run_id,
                ticks=ticks,
                entity_ids=entity_ids,
                active_only=True,
                commit_tokens=commit_tokens,
            )
            result = result.concat(df.select(*proj_cols))

        return result

    async def list_signatures(self) -> list[ArchetypeSignature]:
        """Delegate to the underlying store's signature registry."""
        return await self._store.list_signatures()

    async def _validate(self, sig: ArchetypeSignature, df: DataFrame):
        # No-op in baseline; validation lives in instrumentation layer
        return None


def _canonicalize(sig: ArchetypeSignature) -> ArchetypeSignature:
    """Return *sig* sorted by component class name (the canonical form).

    ``Archetype.sig_from_components`` always produces sorted sigs internally.
    This helper lets the query layer accept unsorted tuples from callers
    without exposing a new public API function.
    """
    return tuple(sorted(sig, key=lambda t: t.__name__))
