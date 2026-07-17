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

"""External-boundary helpers for Archetype processors.

Every external boundary in Archetype (MuJoCo physics, robosuite/LIBERO env
step, policy inference) shares a structural pattern:

1. A ``@daft.cls`` with an ``__init__`` that loads expensive state once per
   Daft worker (model, network handle, etc.).
2. A ``@daft.method.batch`` that converts each input ``Series`` to a Python
   list, calls the external system, and returns a struct ``Series``.
3. An ``AsyncProcessor.process()`` that calls the UDF via ``col(...)``
   expressions, unpacks the struct into individual columns, and excludes the
   scratch column.

This module provides three utilities:

``series_to_rows(col_names, *series_args) -> list[dict]``
    Convert ordered Series arguments to a list of row dicts.  Reduces the
    per-column Series-to-list boilerplate in every UDF body.
    For use inside ``@daft.method.batch`` bodies only (per-batch boundary;
    not a DataFrame collect).

``external_call_indices(rows) -> list[int]``
    Select rows whose external episode may advance. Inactive and completed
    rows remain pass-through state and never trigger an RPC or inference call.

``unpack_struct(df, scratch, output_map) -> DataFrame``
    Unpack a struct column produced by a boundary UDF back into individual
    columns and exclude the scratch column.  Eliminates the repeated
    ``with_column(name, col(scratch)[field])`` + ``exclude(scratch)``
    boilerplate in every ``process()`` method.

Design rationale
----------------
``@daft.cls`` must be a module-level importable class for pickle to work
across Daft workers.  Dynamically created ``@daft.cls`` instances (produced
by a factory function) fail to pickle.  Therefore this module does *not*
attempt to create ``@daft.cls`` instances dynamically; instead, each boundary
module defines its own ``@daft.cls`` subclass (module-level, importable) and
delegates boilerplate to these two utilities.

Pickling invariants:
- ``series_to_rows`` is a plain function — no state.
- ``unpack_struct`` is a plain function — operates on the DataFrame at the
  processor (driver) side, never serialised to a worker.
- The ``config`` passed to each ``@daft.cls.__init__`` must be picklable
  (scalars, dataclasses, tuples); live handles belong in ``__init__`` itself.

Once-per-worker init invariants:
- Each ``@daft.cls.__init__`` (in the boundary modules) is called once per
  Daft worker.  Expensive state (MuJoCo model, Modal stub, network socket)
  is created there, after any unpickling of config scalars.

Inactive/done-row freeze and pass-through:
- Each stateful UDF receives ``is_active`` and ``done`` (or an equivalent
  terminal column) and freezes rows that fail either lifecycle predicate.
  ``series_to_rows`` includes the declared inputs in each row dict;
  ``external_call_indices`` centralizes which rows may cross the boundary.
"""

from __future__ import annotations

from typing import Any

from daft import Series


def series_to_rows(col_names: list[str], *series_args: Series) -> list[dict[str, Any]]:
    """Convert ordered Series arguments into a list of row dicts.

    Intended for use inside ``@daft.method.batch`` UDF bodies at an
    external-system boundary (MuJoCo, env RPC, policy inference).  The Daft
    executor has already materialised the batch to call the UDF, so each
    Series-to-list conversion is a per-batch access, not a DataFrame collect.

    Parameters
    ----------
    col_names:
        Ordered list of column names, parallel to ``series_args``.
    *series_args:
        One ``Series`` per column, in the same order as ``col_names``.

    Returns
    -------
    List of ``{col_name: value, ...}`` dicts, one per row.

    Example
    -------
    ::

        @daft.method.batch(return_dtype=MY_STRUCT)
        def step(self, done: Series, qpos: Series, qvel: Series) -> Series:
            rows = series_to_rows(["done", "qpos", "qvel"], done, qpos, qvel)
            out = []
            for row in rows:
                if row["done"]:
                    out.append({...passthrough...})
                else:
                    out.append({...compute...})
            return Series.from_pylist(out)
    """
    if len(col_names) != len(series_args):
        raise ValueError(
            f"series_to_rows: col_names has {len(col_names)} entries "
            f"but {len(series_args)} Series were passed"
        )
    # Materialise each Series to Python list (per-batch boundary; justified by
    # UDF executor having already materialised the batch).
    lists = [s.to_pylist() for s in series_args]
    n = len(lists[0]) if lists else 0
    return [{col_names[j]: lists[j][i] for j in range(len(col_names))} for i in range(n)]


def external_call_indices(rows: list[dict[str, Any]]) -> list[int]:
    """Return rows whose active, unfinished external episode may advance."""
    return [i for i, row in enumerate(rows) if row["is_active"] and not row["done"]]


def unpack_struct(
    df: Any,
    scratch: str,
    output_map: dict[str, str],
) -> Any:
    """Unpack a struct column into individual DataFrame columns.

    Applies ``with_column(out_col, col(scratch)[field])`` for each
    ``(field, out_col)`` pair, then excludes the scratch column.  The
    scratch column must already exist in ``df`` (typically placed there by
    calling a boundary UDF with ``df.with_column(scratch, udf(...))``.

    Parameters
    ----------
    df:
        DataFrame containing ``scratch`` as a struct column.
    scratch:
        Name of the temporary struct column produced by the boundary UDF.
    output_map:
        Mapping from struct field name → target DataFrame column name.
        Order does not matter; all fields are unpacked.

    Returns
    -------
    DataFrame with target columns updated and ``scratch`` excluded.

    Example
    -------
    ::

        df = df.with_column("_mj_next", stepper.step(...))
        return unpack_struct(df, "_mj_next", {
            "cart_pos":   "cartpolestate__cart_pos",
            "pole_angle": "cartpolestate__pole_angle",
            "cart_vel":   "cartpolestate__cart_vel",
            "pole_vel":   "cartpolestate__pole_vel",
        })
    """
    from daft import col  # local import avoids circular risk at module level

    for field, out_col in output_map.items():
        df = df.with_column(out_col, col(scratch)[field])
    return df.exclude(scratch)
