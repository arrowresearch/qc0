from __future__ import annotations

from typing import Dict, Tuple, Callable, Any, Union, List
from functools import singledispatch
from enum import IntEnum

import sqlalchemy as sa

from .base import Struct
from .syn import Field


class Cardinality(IntEnum):
    ONE = 1
    SEQ = 2

    def __mul__(self, o: Cardinality):
        assert isinstance(o, Cardinality)
        return self if self >= o else o


class Scope(Struct):
    pass


class EmptyScope(Scope):
    pass


class UnivScope(Scope):
    tables: Dict[str, sa.Table]


class TableScope(Scope):
    tables: Dict[str, sa.Table]
    table: sa.Table


class RecordScope(Scope):
    scope: Scope
    fields: List[Field]


class SyntheticScope(Scope):
    def lookup(self, name) -> Tuple[Scope, Callable[Any, Any]]:
        raise NotImplementedError()  # pragma: no cover


class JsonScope(SyntheticScope):
    def lookup(self, name):
        return self, lambda v: v[name]


class DateScope(SyntheticScope):
    def lookup(self, name):
        names = {
            "year": lambda v: sa.extract("year", v),
            "month": lambda v: sa.extract("month", v),
            "day": lambda v: sa.extract("day", v),
        }
        return EmptyScope(), names[name]


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
