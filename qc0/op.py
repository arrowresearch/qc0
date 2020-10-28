"""

    qc0.op
    ======

    This module defines operations. Operations represents some concrete actions
    which needs to be done when running a query - they already know about the
    entities they operate on (which tables or columns they are fetching).

"""

from __future__ import annotations
from typing import Optional, Any, Callable, Dict, List
from sqlalchemy import Table, Column, ForeignKey
from .base import Struct, undefined
from .scope import Scope, Cardinality


class Op(Struct):
    """ Base class for ops."""

    scope: Scope
    card: Cardinality

    def __yaml__(self):
        rep = super(Op, self).__yaml__()
        if "scope" in rep:
            rep.pop("scope")
        return rep

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
    parent: ExprRel


class RelAggregateParent(Rel):
    pass


class RelTake(Rel):
    rel: Rel
    take: Expr


class RelFilter(Rel):
    rel: Rel
    expr: Expr


class RelSort(Rel):
    rel: Rel
    args: List[Expr]


class RelGroup(Rel):
    rel: Rel
    fields: Dict[str, Field]
    aggregates: Dict[str, Field]


class Expr(Op):
    """ Base class for ops which expr a new value."""


class ExprRel(Expr):
    rel: Rel
    expr: Expr

    def replace_expr(self, expr):
        return self.replace(expr=expr, scope=expr.scope, card=expr.card)

    def replace_rel(self, rel, expr=undefined):
        if expr is undefined:
            expr = self.expr
        return self.replace(expr=expr, rel=rel, scope=rel.scope, card=rel.card)


class ExprRecord(Expr):
    fields: Dict[str, Field]


class ExprColumn(Expr):
    column: Column


class ExprIdentity(Expr):
    table: Table


class ExprAggregateRel(Expr):
    rel: Rel
    expr: Expr
    func: Optional[str]
    unit: Any


class ExprConst(Expr):
    value: Any
    embed: Callable[[Any], Any]


class ExprApply(Expr):
    expr: Expr
    args: List[Expr]
    compile: Callable[[Expr, List[Expr]], Any]


class Field(Struct):
    name: str
    expr: Expr
