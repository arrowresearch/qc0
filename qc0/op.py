"""

    qc0.op
    ======

    This module defines operations. Operations represents some concrete actions
    which needs to be done when running a query - they already know about the
    entities they operate on (which tables or columns they are fetching).

"""

from __future__ import annotations
from typing import List
from sqlalchemy import Table, Column, ForeignKey
from .base import Struct


class Op(Struct):
    """ Base class for ops."""


class Pipe(Op):
    """ Base class for ops which query data."""


class PipeTable(Pipe):
    table: Table


class PipeJoin(Pipe):
    pipe: Pipe
    fk: ForeignKey


class PipeRevJoin(Pipe):
    pipe: Pipe
    fk: ForeignKey


class PipeColumn(Pipe):
    pipe: Pipe
    column: Column


class PipeParent(Pipe):
    pass


class PipeExpr(Pipe):
    pipe: Pipe
    expr: Expr


class Expr(Op):
    """ Base class for ops which expr a new value."""


class ExprRecord(Expr):
    fields: List[Field]


class ExprColumn(Expr):
    column: Column


class ExprPipe(Expr):
    pipe: Pipe


class ExprAggregatePipe(Expr):
    pipe: Pipe


class Field(Struct):
    name: str
    expr: Expr
