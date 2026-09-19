"""Schema evolution detection (requirement 13).

We compare the schema of an incoming batch against the current Silver/Iceberg
table schema and classify the difference:

    * ADDITIVE  — new columns appeared (safe: Iceberg supports add-column).
                  Also widening promotions (int -> long, float -> double,
                  decimal precision increase) are additive/compatible.
    * BREAKING  — a column was dropped, renamed, or its type changed in an
                  incompatible way (e.g. string -> int). These must halt the
                  affected table's processing, record an error and notify,
                  rather than silently corrupting data.
    * NONE      — schemas are equivalent.

The functions operate on lists of ``(name, type_string)`` tuples so they are
fully unit-testable without a live catalog. A thin ``diff_dataframe_schema``
helper adapts a Spark ``StructType``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

# Allowed "widening" promotions that Iceberg / Spark treat as compatible.
_COMPATIBLE_PROMOTIONS = {
    ("int", "long"),
    ("integer", "long"),
    ("int", "bigint"),
    ("float", "double"),
    ("date", "timestamp"),
}


@dataclass
class SchemaDiff:
    added: List[Tuple[str, str]] = field(default_factory=list)  # (name, type)
    removed: List[str] = field(default_factory=list)  # column names
    type_changed: List[Tuple[str, str, str]] = field(default_factory=list)  # (name, old, new)
    incompatible: List[Tuple[str, str, str]] = field(default_factory=list)  # breaking subset

    @property
    def is_breaking(self) -> bool:
        return bool(self.removed or self.incompatible)

    @property
    def is_additive(self) -> bool:
        return bool(self.added) and not self.is_breaking

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.type_changed)

    def summary(self) -> str:
        bits = []
        if self.added:
            bits.append("added=" + ",".join(n for n, _ in self.added))
        if self.removed:
            bits.append("removed=" + ",".join(self.removed))
        if self.incompatible:
            bits.append(
                "incompatible=" + ",".join(f"{n}({o}->{t})" for n, o, t in self.incompatible)
            )
        elif self.type_changed:
            bits.append("promoted=" + ",".join(f"{n}({o}->{t})" for n, o, t in self.type_changed))
        return "; ".join(bits) or "no change"


def _norm(t: str) -> str:
    return t.strip().lower()


def diff_schema(
    current: List[Tuple[str, str]],
    incoming: List[Tuple[str, str]],
) -> SchemaDiff:
    """Compute a :class:`SchemaDiff` between current and incoming schemas.

    ``current`` / ``incoming`` are ordered lists of ``(column_name, type)``.
    Control columns (prefixed ``__``) are ignored — they are managed by the
    pipeline, not the source.
    """
    cur: Dict[str, str] = {n: _norm(t) for n, t in current if not n.startswith("__")}
    inc: Dict[str, str] = {n: _norm(t) for n, t in incoming if not n.startswith("__")}

    diff = SchemaDiff()

    for name, t in inc.items():
        if name not in cur:
            diff.added.append((name, t))

    for name in cur:
        if name not in inc:
            # Column disappeared from the source feed → breaking (drop/rename).
            diff.removed.append(name)

    for name, new_t in inc.items():
        if name in cur and cur[name] != new_t:
            old_t = cur[name]
            diff.type_changed.append((name, old_t, new_t))
            if (old_t, new_t) not in _COMPATIBLE_PROMOTIONS:
                diff.incompatible.append((name, old_t, new_t))

    return diff


def struct_to_pairs(struct_type) -> List[Tuple[str, str]]:
    """Convert a Spark ``StructType`` to ``[(name, simpleString-type), ...]``."""
    return [(f.name, f.dataType.simpleString()) for f in struct_type.fields]


def diff_dataframe_schema(current_struct, incoming_struct) -> SchemaDiff:
    """Convenience wrapper for two Spark ``StructType`` objects."""
    return diff_schema(struct_to_pairs(current_struct), struct_to_pairs(incoming_struct))
