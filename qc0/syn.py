"""

    qc0.syn
    =======

    This module defines syntax for Query Combinators.

"""

from __future__ import annotations

from functools import singledispatch
from datetime import date
from typing import List, Optional, Any

from sqlalchemy.dialects import postgresql as sa_pg
import sqlalchemy as sa

from .base import Struct


class Syn(Struct):
    """ Base class for representing syntax."""


class Nav(Syn):
    """
    Navigate to NAME at ROOT:

        ROOT . NAME

    """

    parent: Optional[Syn]
    name: str


class Select(Syn):
    """
    Attach multiple queries FIELD... at ROOT:

        ROOT { FIELD... }

    """

    parent: Optional[Syn]
    fields: List[Field]


class Field(Struct):
    """
    Field is a named query (used in select):

        NAME := QUERY

    """

    name: str
    syn: Syn


class Apply(Syn):
    """
    Apply some query combinator:

        NAME(ARG1, ARG2)

    """

    name: str
    args: List[Syn]


class Literal(Syn):
    """
    Represent literal values.
    """

    value: Any
    type: sa.Type


class Compose(Syn):
    """
    Composition of two queries:

        Q1.Q1

    """

    a: Syn
    b: Syn


class Q:
    """ Convenience to build syntax trees in Python."""

    def __init__(self, syn):
        self.syn = syn

    def __getattr__(self, name):
        return self.__class__(Nav(parent=self.syn, name=name))

    def select(self, **fields):
        fields = {
            name: Field(syn=syn.syn, name=name) for name, syn in fields.items()
        }
        return self.__class__(Select(parent=self.syn, fields=fields))

    def val(self, v):
        return make_value(v, query_cls=self.__class__)

    def json_val(self, v):
        return self.__class__(Literal(value=v, type=sa_pg.JSONB()))

    def __eq__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Apply(name="__eq__", args=(self.syn, o.syn)))

    def __ne__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Apply(name="__ne__", args=(self.syn, o.syn)))

    def __add__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Apply(name="__add__", args=(self.syn, o.syn)))

    def __sub__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Apply(name="__sub__", args=(self.syn, o.syn)))

    def __mul__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Apply(name="__mul__", args=(self.syn, o.syn)))

    def __truediv__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(
            Apply(name="__truediv__", args=(self.syn, o.syn))
        )

    def __and__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Apply(name="__and__", args=(self.syn, o.syn)))

    def __or__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Apply(name="__or__", args=(self.syn, o.syn)))

    def __rshift__(self, o: Q):
        assert isinstance(o, Q)
        return self.__class__(Compose(a=self.syn, b=o.syn))

    def __call__(self, *args):
        assert isinstance(self.syn, Nav)
        args = tuple(arg.syn if isinstance(arg, Q) else arg for arg in args)
        if self.syn.parent is not None:
            args = (self.syn.parent,) + args
        return self.__class__(Apply(name=self.syn.name, args=args))


q = Q(None)


@singledispatch
def make_value(v, query_cls):
    raise NotImplementedError(  # pragma: no cover
        f"unable to use {type(v)} as literal query"
    )


@make_value.register
def int_make_value(v: int, query_cls):
    return query_cls(Literal(value=v, type=sa.Integer()))


@make_value.register
def str_make_value(v: str, query_cls):
    return query_cls(Literal(value=v, type=sa.String()))


@make_value.register
def bool_make_value(v: bool, query_cls):
    return query_cls(Literal(value=v, type=sa.Boolean()))


@make_value.register
def dict_make_value(v: dict, query_cls):
    return query_cls(Literal(value=v, type=sa_pg.JSONB()))


@make_value.register
def list_make_value(v: list, query_cls):
    return query_cls(Literal(value=v, type=sa_pg.JSONB()))


@make_value.register
def date_make_value(v: date, query_cls):
    return query_cls(Literal(value=v, type=sa.Date()))
