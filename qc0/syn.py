"""

    qc0.syn
    =======

    This module defines syntax for Query Combinators.

"""

from __future__ import annotations
from datetime import date
from typing import List, Optional, Any, get_type_hints
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
    def make(cls, value):
        impls = cls.__subclasses__()
        for impl in impls:
            hints = get_type_hints(impl)
            impl_type = hints["value"]
            if isinstance(value, impl_type):
                return impl(value)
        assert False, f"Unable to embed value of type {type(value)} into query"

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
        "year": lambda v: sa.extract("year", v),
        "month": lambda v: sa.extract("month", v),
        "day": lambda v: sa.extract("day", v),
    }

    @staticmethod
    def embed(v):
        v = v.strftime("%Y-%m-%d")
        return sa.cast(sa.literal(v), sa.Date)


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


def literal(value):
    return Q(Literal.make(value))
