"""

    qc0.syn
    =======

    This module defines syntax for Query Combinators.

"""

from __future__ import annotations

from functools import singledispatch
from datetime import date
from typing import Dict, List, Union, Any

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg

from .base import Struct


class Syn(Struct):
    """ Base class for representing syntax."""


class Nav(Syn):
    """
    Navigate to NAME at ROOT:

        ROOT . NAME

    """

    name: str


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
    args: Union[Dict[str, Field], List[Syn]]


class Literal(Syn):
    """
    Represent literal values.
    """

    value: Any
    type: Any


class Compose(Syn):
    """
    Composition of two queries:

        Q1.Q1

    """

    a: Syn
    b: Syn


class BinOp(Syn):
    """
    Binary operation:

        Q1 OP Q1

    """

    op: str
    a: Syn
    b: Syn


class Desc(Syn):
    """
    Descending sort modifier:

        Q1-

    """

    syn: Syn


@singledispatch
def make_value(v):
    raise NotImplementedError(  # pragma: no cover
        f"unable to use {type(v)} as literal query"
    )


@make_value.register
def int_make_value(v: int):
    return Literal(value=v, type=sa.Integer())


@make_value.register
def float_make_value(v: float):
    return Literal(value=v, type=sa.Float())


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
