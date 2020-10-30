"""

    qc0.op
    ======

    This module defines operations. Operations represents some concrete actions
    which needs to be done when running a query - they already know about the
    entities they operate on (which tables or columns they are fetching).

"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional
from sqlalchemy import Table, ForeignKey
from sqlalchemy.sql.elements import ColumnClause
from .base import Struct, undefined
from .scope import Scope, Cardinality


class Op(Struct):
    scope: Scope
    card: Cardinality
    rel: Rel
    expr: Optional[Expr]

    def grow_expr(self, expr, scope=undefined, card=undefined):
        card = self.card if card is undefined else card
        scope = self.scope if scope is undefined else scope
        return self.replace(expr=expr, scope=scope, card=card)

    def grow_rel(self, rel, scope=undefined, card=undefined):
        card = self.card if card is undefined else card
        scope = self.scope if scope is undefined else scope
        return self.replace(rel=rel, scope=scope, card=card)

    def __yaml__(self):
        rep = super(Op, self).__yaml__()
        if "scope" in rep:
            rep.pop("scope")
        return rep


class Rel(Struct):
    """ Base class for ops which query data."""


class RelVoid(Rel):
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
    parent: Op


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
    sort: List[Sort]


class RelGroup(Rel):
    rel: Rel
    fields: Dict[str, Field]
    aggregates: Dict[str, Field]


class Expr(Struct):
    """ Base class for ops which expr a new value."""


class ExprOp(Expr):
    op: Op


class ExprOpAggregate(Expr):
    op: Op
    sig: Any


class ExprRecord(Expr):
    fields: Dict[str, Field]


class ExprColumn(Expr):
    column: ColumnClause


class ExprIdentity(Expr):
    table: Table


class ExprConst(Expr):
    value: Any
    embed: Callable[[Any], Any]


class ExprApply(Expr):
    expr: Optional[Expr]
    args: List[Expr]
    compile: Callable[[Expr, List[Expr]], Any]


class Field(Struct):
    name: str
    expr: Expr


class Sort(Struct):
    expr: Expr
    desc: bool
