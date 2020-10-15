"""

    qc0.syn
    =======

    This module defines syntax for Query Combinators.

"""

from __future__ import annotations

from functools import singledispatch
from datetime import date
from typing import List, Any

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

    name: str


class Select(Syn):
    """
    Attach multiple queries FIELD... at ROOT:

        ROOT { FIELD... }

    """

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
        nav = Nav(name=name)
        if self.syn is None:
            return self.__class__(nav)
        else:
            return self.__class__(Compose(self.syn, nav))

    def select(self, **fields):
        fields = {
            name: Field(syn=syn.syn, name=name) for name, syn in fields.items()
        }
        return self.__class__(Compose(self.syn, Select(fields=fields)))

    def group(self, **fields):
        fields = {
            name: Field(syn=syn.syn, name=name) for name, syn in fields.items()
        }
        return self.__class__(Compose(self.syn, Apply("group", fields)))

    def val(self, v):
        val = make_value(v)
        if self.syn is None:
            return self.__class__(val)
        else:
            return self.__class__(Compose(self.syn, val))

    def json_val(self, v):
        val = Literal(value=v, type=sa_pg.JSONB())
        if self.syn is None:
            return self.__class__(val)
        else:
            return self.__class__(Compose(self.syn, val))

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
        args = tuple(arg.syn if isinstance(arg, Q) else arg for arg in args)
        if isinstance(self.syn, Nav):
            name = self.syn.name
            return self.__class__(Apply(name=name, args=args))
        elif isinstance(self.syn, Compose) and isinstance(self.syn.b, Nav):
            parent = self.syn.a
            name = self.syn.b.name
            return self.__class__(Compose(parent, Apply(name=name, args=args)))
        else:
            assert (  # pragma: no cover
                False
            ), "SyntaxError: cannot do call here"


q = Q(None)


@singledispatch
def make_value(v, query_cls):
    raise NotImplementedError(  # pragma: no cover
        f"unable to use {type(v)} as literal query"
    )


@make_value.register
def int_make_value(v: int):
    return Literal(value=v, type=sa.Integer())


@make_value.register
def str_make_value(v: str):
    return Literal(value=v, type=sa.String())


@make_value.register
def bool_make_value(v: bool):
    return Literal(value=v, type=sa.Boolean())


@make_value.register
def dict_make_value(v: dict):
    return Literal(value=v, type=sa_pg.JSONB())


@make_value.register
def list_make_value(v: list):
    return Literal(value=v, type=sa_pg.JSONB())


@make_value.register
def date_make_value(v: date):
    return Literal(value=v, type=sa.Date())
