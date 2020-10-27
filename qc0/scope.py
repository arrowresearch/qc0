"""

    qc0.scope
    =========

    This module describes query scoping.

"""

from __future__ import annotations

from typing import Dict, Tuple, Callable, Any, Union
from functools import singledispatch
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

    scope: Scope
    fields: Dict[str, Field]

    def __yaml__(self):
        return {"scope": self.scope, "fields": list(self.fields)}


class GroupScope(Scope):
    """
    A scope created by grouping.
    """

    scope: Scope
    fields: Dict[str, Field]
    aggregates: Dict[str, Any]

    def add_aggregate(self, expr):
        idx = len(self.aggregates)
        name = f"aggr_{idx}"
        self.aggregates[name] = expr
        return name

    def __yaml__(self):
        return {"scope": self.scope, "fields": list(self.fields)}


class SyntheticScope(Scope):
    """
    Base class for synthetic scopes.

    Such scopes are "synthetic" in a sense that there's no "physical" structure
    behind them, instead they are being computed by queries.
    """

    def lookup(self, name) -> Tuple[Scope, Callable[Any, Any]]:
        """
        Lookup ``name`` in the scope.

        The return value is a tuple of the next scope (it's valid to be
        ``EmptyScope``) and a function which computes the value in
        ``sqlalchemy`` terms.
        """
        raise NotImplementedError()  # pragma: no cover


class JsonScope(SyntheticScope):
    """
    Scope for JSON values.

    It allows to traverse JSON values using PostgreSQL's ``->`` operator.
    """

    def lookup(self, name):
        return lambda expr, _args: expr[name], sa.dialects.postgresql.JSONB()


class DateScope(SyntheticScope):
    """
    Scope for date values.

    It destructures any date into ``year``, ``month`` and ``day`` integers.
    """

    def lookup(self, name):
        if name not in {"year", "month", "year"}:
            raise LookupError(name)
        return lambda expr, _args: sa.extract(name, expr), sa.Integer()


@singledispatch
def type_scope(_: sa.Type) -> Union[Scope, SyntheticScope]:
    """ Describe scope for a specified type. """
    return EmptyScope()


@type_scope.register
def Date_scalar_scope(_: sa.Date):
    return DateScope()


@type_scope.register
def Json_scalar_scope(_: sa.dialects.postgresql.JSONB):
    return JsonScope()
