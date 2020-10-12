"""

    qc0.op
    ======

    This module defines operations. Operations represents some concrete actions
    which needs to be done when running a query - they already know about the
    entities they operate on (which tables or columns they are fetching).

"""

from __future__ import annotations
from typing import List, Optional, Any, Callable
from sqlalchemy import Table, Column, ForeignKey
from .base import Struct
from .scope import Scope, Cardinality


class Op(Struct):
    """ Base class for ops."""

    scope: Scope
    card: Cardinality

    @classmethod
    def wrap(cls, op: Op, **kw):
        kw = {"scope": op.scope, "card": op.card, **kw}
        return cls(**kw)


class Pipe(Op):
    """ Base class for ops which query data."""


class PipeVoid(Op):
    pass


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


class PipeTake(Pipe):
    pipe: Pipe
    take: int


class PipeFilter(Pipe):
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
    func: Optional[str]


class ExprConst(Expr):
    pipe: Pipe
    value: Any
    embed: Callable[Any, Any]


class ExprTransform(Expr):
    expr: Expr
    transform: Callable[Any, Any]


class ExprBinOp(Expr):
    func: str
    a: Expr
    b: Expr


class Field(Struct):
    name: str
    expr: Expr
