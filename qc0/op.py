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
from .scope import Scope, EmptyScope, Cardinality
from .syntax import Syn, Compose


class Op(Struct):
    scope: Scope
    card: Cardinality
    rel: Rel
    expr: Optional[Expr]
    syn: Optional[Syn] = None
    sig: Any = None

    def grow_expr(self, expr, syn=None, scope=undefined, card=undefined):
        card = self.card if card is undefined else card
        scope = self.scope if scope is undefined else scope
        syn = (
            Compose(self.syn, syn)
            if self.syn and syn
            else self.syn
            if self.syn
            else syn
        )
        return self.replace(expr=expr, scope=scope, card=card, syn=syn)

    def grow_rel(self, rel, syn=None, scope=undefined, card=undefined):
        card = self.card if card is undefined else card
        scope = self.scope if scope is undefined else scope
        syn = (
            Compose(self.syn, syn)
            if self.syn and syn
            else self.syn
            if self.syn
            else syn
        )
        return self.replace(rel=rel, scope=scope, card=card, syn=syn)

    def aggregate(self, sig):
        return self.replace(sig=sig, scope=EmptyScope(), card=Cardinality.ONE)

    def __yaml__(self):
        rep = super(Op, self).__yaml__()
        rep.pop("scope")
        rep.pop("syn")
        return rep


#
# Relations
#


class Rel(Struct):
    """ Operation which queries data from relations."""


class RelFunctor(Rel):
    """ Operation which doesn't affect the shape of the result."""

    def keep(self, name, op):
        return self.rel.keep(name, op)


class RelObject(Rel):
    """ Operation which affects the shape of the result."""

    compute: Optional[List[Field]]

    def keep(self, name, op):
        """Make ``op`` evaluate under ``name`` at the current relation."""
        # TODO(andreypopp): we need to defer naming things till the SQL
        # compilation pass, consider keeping track of kept computations by
        # operation equality
        idx = 0
        name0 = f"{name}_{idx}"
        names = {f.name for f in self.compute}
        while name0 in names:
            name0 = f"{name}_{idx}"
            idx = idx + 1

        field = Field(name=name0, op=op)
        self.compute.append(field)
        return name0


class RelVoid(RelObject):
    pass


class RelTable(RelObject):
    rel: Rel
    table: Table


class RelJoin(RelObject):
    rel: Rel
    fk: ForeignKey


class RelRevJoin(RelObject):
    rel: Rel
    fk: ForeignKey


class RelGroup(RelObject):
    rel: Rel
    fields: Dict[str, Field]


class RelTake(RelFunctor):
    rel: Rel
    take: Op


class RelFilter(RelFunctor):
    rel: Rel
    cond: Op


class RelSort(RelFunctor):
    rel: Rel
    sort: List[Sort]


class RelParent(Rel):
    parent: Op

    def keep(self, name, op):
        return self.parent.rel.keep(name, op)


class RelAggregateParent(Rel):
    pass


class RelAroundParent(Rel):
    pass


#
# Expressions
#


class Expr(Struct):
    """ Base class for ops which expr a new value."""


class ExprOp(Expr):
    op: Op


class ExprRecord(Expr):
    fields: Dict[str, Field]


class ExprColumn(Expr):
    column: ColumnClause


class ExprCompute(Expr):
    name: str


class ExprIdentity(Expr):
    table: Table


class ExprConst(Expr):
    value: Any
    embed: Callable[[Any], Any]


class ExprApply(Expr):
    expr: Optional[Expr]
    args: List[Expr]
    compile: Callable[[Expr, List[Expr]], Any]


#
# Aux structures
#


class Field(Struct):
    name: str
    op: Op


class Sort(Struct):
    op: Op
    desc: bool
