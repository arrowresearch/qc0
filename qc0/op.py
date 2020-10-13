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


class Rel(Op):
    """ Base class for ops which query data."""


class RelVoid(Op):
    pass


class RelTable(Rel):
    table: Table


class RelJoin(Rel):
    rel: Rel
    fk: ForeignKey


class RelRevJoin(Rel):
    rel: Rel
    fk: ForeignKey


class RelParent(Rel):
    pass


class RelExpr(Rel):
    rel: Rel
    expr: Expr


class RelTake(Rel):
    rel: Rel
    take: int


class RelFilter(Rel):
    rel: Rel
    expr: Expr


class Expr(Op):
    """ Base class for ops which expr a new value."""


class ExprRecord(Expr):
    fields: List[Field]


class ExprColumn(Expr):
    column: Column


class ExprIdentity(Expr):
    table: Table


class ExprRel(Expr):
    rel: Rel


class ExprAggregateRel(Expr):
    rel: Rel
    func: Optional[str]


class ExprConst(Expr):
    rel: Rel
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
