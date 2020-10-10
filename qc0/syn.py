"""

    qc0.syn
    =======

    This module defines syntax for Query Combinators.

"""

from __future__ import annotations

from functools import singledispatch
from json import dumps as json_dumps
from datetime import date
from typing import Union, List, Optional, Any

import sqlalchemy as sa

from .base import Struct


class Syn(Struct):
    """ Base class for representing syntax."""

    def to_op(self, ctx):
        """ Produce op out of a query."""
        raise NotImplementedError()


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
    Represent literal values
    """

    value: Any

    scope = None

    @classmethod
    def embed(cls, value):
        raise NotImplementedError()


class StringLiteral(Literal):
    value: str
    embed = staticmethod(sa.literal)


class IntegerLiteral(Literal):
    value: int
    embed = staticmethod(sa.literal)


class BooleanLiteral(Literal):
    value: bool
    embed = staticmethod(sa.literal)


class DateLiteral(Literal):
    value: date

    scope = {
        "year": (None, (lambda v: sa.extract("year", v))),
        "month": (None, (lambda v: sa.extract("month", v))),
        "day": (None, (lambda v: sa.extract("day", v))),
    }

    @staticmethod
    def embed(v):
        v = v.strftime("%Y-%m-%d")
        return sa.cast(sa.literal(v), sa.Date)


class JsonScope:
    def __getitem__(self, key):
        return self, lambda v: v[key]


class JsonLiteral(Literal):
    value: Union[dict, list]

    scope = JsonScope()

    @staticmethod
    def embed(v):
        v = json_dumps(v)
        return sa.cast(sa.literal(v), sa.dialects.postgresql.JSONB)


class Q:
    """ Convenience to build syntax trees in Python."""

    def __init__(self, syn):
        self.syn = syn

    def __getattr__(self, name):
        return Q(Nav(parent=self.syn, name=name))

    def select(self, **fields):
        fields = {
            name: Field(syn=syn.syn, name=name) for name, syn in fields.items()
        }
        return Q(Select(parent=self.syn, fields=fields))

    def __eq__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__eq__", args=(self.syn, o.syn)))

    def __ne__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__ne__", args=(self.syn, o.syn)))

    def __add__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__add__", args=(self.syn, o.syn)))

    def __sub__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__sub__", args=(self.syn, o.syn)))

    def __mul__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__mul__", args=(self.syn, o.syn)))

    def __truediv__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__truediv__", args=(self.syn, o.syn)))

    def __and__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__and__", args=(self.syn, o.syn)))

    def __or__(self, o: Q):
        assert isinstance(o, Q)
        return Q(Apply(name="__or__", args=(self.syn, o.syn)))

    def __call__(self, *args):
        assert isinstance(self.syn, Nav)
        args = tuple(arg.syn if isinstance(arg, Q) else arg for arg in args)
        if self.syn.parent is not None:
            args = (self.syn.parent,) + args
        return Q(Apply(name=self.syn.name, args=args))


q = Q(None)


@singledispatch
def literal(v):
    raise NotImplementedError(f"unable to use {type(v)} as literal query")


@literal.register
def int_literal(v: int):
    return Q(IntegerLiteral(v))


@literal.register
def str_literal(v: str):
    return Q(StringLiteral(v))


@literal.register
def bool_literal(v: bool):
    return Q(BooleanLiteral(v))


@literal.register
def dict_literal(v: dict):
    return Q(JsonLiteral(v))


@literal.register
def list_literal(v: list):
    return Q(JsonLiteral(v))


@literal.register
def date_literal(v: date):
    return Q(DateLiteral(v))


def json_literal(value):
    return Q(JsonLiteral(value))
