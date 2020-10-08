"""

    qc0.syn
    =======

    This module defines syntax for Query Combinators.

"""

from __future__ import annotations
from typing import List, Optional
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


q = Q(None)
