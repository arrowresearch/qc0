"""

    qc0.scope
    =========

    This module describes query scoping.

"""

from __future__ import annotations

from typing import Dict, Any
from enum import IntEnum

import sqlalchemy as sa

from .base import Struct
from .syntax import Field


class Cardinality(IntEnum):
    ONE = 1
    SEQ = 2

    def __mul__(self, o: Cardinality):
        assert isinstance(o, Cardinality)
        return self if self >= o else o


class Scope(Struct):
    pass


class EmptyScope(Scope):
    """ There's nowhere to navigate from this point."""


class UnivScope(Scope):
    """
    A universe/initial scope.

    One can navigate to any db tables from this point.
    """

    tables: Dict[str, sa.Table]


class TableScope(Scope):
    """
    A table scope.

    One can navigate to any of the table columns, fk relationships and reverse
    fk relationships.
    """

    table: sa.Table

    @property
    def foreign_keys(self):
        return {fk.column.table.name: fk for fk in self.table.foreign_keys}

    @property
    def rev_foreign_keys(self):
        # TODO(andreypopp): this is silly to do on each lookup, we should
        # organize this better
        return {
            fk.parent.table.name: fk
            for t in self.table.metadata.tables.values()
            for fk in t.foreign_keys
            if fk.column.table == self.table
        }

    def __yaml__(self):
        return {"table": str(self.table.name)}


class RecordScope(Scope):
    """
    A scope created by selection.
    """

    parent: Any
    fields: Dict[str, Field]

    def __yaml__(self):
        return {"parent": self.parent, "fields": list(self.fields)}


class GroupScope(Scope):
    """
    A scope created by grouping.
    """

    scope: Scope
    fields: Dict[str, Field]

    def __yaml__(self):
        return {"scope": self.scope, "fields": list(self.fields)}
