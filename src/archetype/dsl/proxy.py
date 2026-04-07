"""AgentProxy — attribute access over a DataFrame row dict."""

from __future__ import annotations


class AgentProxy:
    """Read-only proxy over a single entity's row dict.

    Provides attribute access that resolves prefixed column names::

        agent.name       -> row["agent__name"]
        agent.skill      -> row["agent__skill"]
        agent.entity_id  -> row["entity_id"]
    """

    __slots__ = ("_row", "_prefixes")

    def __init__(self, row: dict, prefixes: list[str]) -> None:
        object.__setattr__(self, "_row", row)
        object.__setattr__(self, "_prefixes", prefixes)

    @property
    def entity_id(self) -> int:
        return self._row["entity_id"]

    def __getattr__(self, name: str):
        row = self._row
        # Try each component prefix
        for prefix in self._prefixes:
            key = f"{prefix}__{name}"
            if key in row:
                return row[key]
        # Fall back to direct key (metadata columns)
        if name in row:
            return row[name]
        raise AttributeError(f"AgentProxy has no attribute '{name}'")

    def __repr__(self) -> str:
        eid = self._row.get("entity_id", "?")
        fields = []
        for key, val in self._row.items():
            if "__" in key and not key.startswith("_"):
                short = key.split("__", 1)[1]
                fields.append(f"{short}={val!r}")
        return f"Agent(entity_id={eid}, {', '.join(fields)})"
