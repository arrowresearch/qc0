"""

    qc0.syn_to_op
    =============

    Produce operations out of syntax.

"""

from __future__ import annotations

import json
import functools
import typing as ty

import sqlalchemy as sa

from .scope import (
    Cardinality,
    EmptyScope,
    UnivScope,
    TableScope,
    RecordScope,
    SyntheticScope,
    type_scope,
)
from .syn import (
    Syn,
    Nav,
    Compose,
    Select,
    Apply,
    Literal,
)
from .op import (
    Op,
    Rel,
    RelVoid,
    RelTable,
    RelJoin,
    RelRevJoin,
    RelParent,
    RelExpr,
    RelTake,
    RelFilter,
    Expr,
    ExprRel,
    ExprAggregateRel,
    ExprRecord,
    ExprColumn,
    ExprIdentity,
    ExprConst,
    ExprBinOp,
    ExprTransform,
    Field,
)


def syn_to_op(syn: Syn, meta: sa.MetaData):
    """ Produce operations from syntax."""
    parent = RelVoid(
        scope=UnivScope(tables=meta.tables),
        card=Cardinality.ONE,
    )
    return build_op(syn, parent)


def build_op(syn: Syn, parent: Op):
    op = to_op(syn, parent=parent)
    op = wrap_expr(op)
    return op


def wrap_expr(op: Op):
    if isinstance(op.scope, RecordScope):
        parent = RelParent(scope=op.scope.scope, card=Cardinality.ONE)
        fields = {}
        for name, f in op.scope.fields.items():
            expr = build_op(f.syn, parent)
            if isinstance(expr, Rel):
                if expr.card == Cardinality.SEQ:
                    expr = ExprAggregateRel(
                        rel=expr,
                        func=None,
                        card=Cardinality.ONE,
                        scope=EmptyScope(),
                    )
                else:
                    expr = ExprRel.wrap(expr, rel=expr)
            fields[name] = Field(expr=expr, name=name)

        expr = ExprRecord.wrap(op, fields=fields)
        return RelExpr.wrap(op, rel=op, expr=expr)
    if isinstance(op.scope, TableScope):
        scope = EmptyScope()
        expr = ExprIdentity.wrap(op, table=op.scope.table, scope=scope)
        return RelExpr.wrap(op, rel=op, expr=expr, scope=scope)
    return op


@functools.singledispatch
def to_op(syn: ty.Optional[Syn], parent: Op):
    """ Produce an operation out of a query."""
    raise NotImplementedError(type(syn))  # pragma: no cover


@to_op.register
def None_to_op(syn: type(None), parent: Op):
    return parent


@to_op.register
def Nav_to_op(syn: Nav, parent: Op):

    if isinstance(parent.scope, UnivScope):
        table = parent.scope.tables[syn.name]
        return RelTable(
            table=table,
            scope=TableScope(table=table),
            card=Cardinality.SEQ,
        )

    elif isinstance(parent.scope, TableScope):
        table = parent.scope.table

        if syn.name in table.columns:
            column = table.columns[syn.name]
            next_scope = type_scope(column.type)
            next_card = parent.card * Cardinality.ONE
            return RelExpr(
                scope=next_scope,
                card=next_card,
                rel=parent,
                expr=ExprColumn(
                    column=column,
                    scope=next_scope,
                    card=next_card,
                ),
            )

        fk = parent.scope.foreign_keys.get(syn.name)
        if fk:
            return RelJoin(
                rel=parent,
                fk=fk,
                scope=TableScope(table=fk.column.table),
                card=parent.card * Cardinality.ONE,
            )

        fk = parent.scope.rev_foreign_keys.get(syn.name)
        if fk:
            return RelRevJoin(
                rel=parent,
                fk=fk,
                scope=TableScope(table=fk.parent.table),
                card=parent.card * Cardinality.SEQ,
            )

        assert False, f"Unable to lookup {syn.name}"  # pragma: no cover

    elif isinstance(parent.scope, RecordScope):
        if syn.name in parent.scope.fields:
            field = parent.scope.fields[syn.name]
            return to_op(
                field.syn, parent=parent.replace(scope=parent.scope.scope)
            )
        else:
            names = ", ".join(parent.scope.fields)  # pragma: no cover
            assert (  # pragma: no cover
                False
            ), f"Unable to lookup {syn.name} in record scope, names: {names}"

    elif isinstance(parent.scope, SyntheticScope):
        transform, type = parent.scope.lookup(syn.name)
        next_scope = type_scope(type)
        assert transform is not None, f"Unable to lookup {syn.name}"
        if isinstance(parent, Rel):
            parent = ExprRel.wrap(parent, rel=parent)
        return ExprTransform.wrap(
            parent, expr=parent, transform=transform, scope=next_scope
        )

    elif isinstance(parent.scope, EmptyScope):  # pragma: no cover
        assert (
            False
        ), f"Unable to lookup {syn.name} in empty scope"  # pragma: no cover

    else:
        assert False  # pragma: no cover


@to_op.register
def Select_to_op(syn: Select, parent: Op):
    # In general select() results in ExprRecord but we don't create it here as
    # the next syntax might make such op useless, consider the folloing cases:
    #
    #     region{name: name}.name
    #     region{name: name}{region_name: name}
    #
    # See wrap_expr where we create ExprRecord instead for the selects
    # which are "final".
    scope = RecordScope(scope=parent.scope, fields=syn.fields)
    return parent.replace(scope=scope)


@to_op.register
def Apply_to_op(syn: Apply, parent: Op):
    if syn.name in {"count", "exists", "sum"}:
        assert len(syn.args) == 0, f"{syn.name}(...): expected no arguments"
        assert isinstance(parent, Rel), f"{syn.name}(...): requires a rel"
        assert (
            parent.card >= Cardinality.SEQ
        ), "{syn.name}(...): expected a sequence of items"
        return ExprAggregateRel.wrap(
            parent, rel=parent, func=syn.name, scope=EmptyScope()
        )
    elif syn.name == "take":
        assert len(syn.args) == 1, "take(...): expected a single argument"
        take = syn.args[0]
        assert isinstance(parent, Rel), "take(...): requires a rel"
        return RelTake.wrap(parent, rel=parent, take=take)
    elif syn.name in {
        "__eq__",
        "__ne__",
        "__add__",
        "__sub__",
        "__mul__",
        "__truediv__",
        "__and__",
        "__or__",
    }:
        assert (
            len(syn.args) == 2
        ), f"{syn.name}(...): expected exactly two arguments"
        a, b = syn.args
        a = to_op(a, parent)
        if isinstance(a, Rel):
            a = ExprRel.wrap(a, rel=a)
        b = to_op(b, parent)
        if isinstance(b, Rel):
            b = ExprRel.wrap(b, rel=b)
        return ExprBinOp.wrap(
            parent,
            func=syn.name,
            a=a,
            b=b,
            scope=EmptyScope(),
        )
    elif syn.name == "filter":
        assert len(syn.args) == 1, "filter(...): expected a single argument"
        expr = syn.args[0]
        assert isinstance(parent, Rel), "filter(...): requires a rel"
        expr = to_op(
            expr,
            RelParent(scope=parent.scope, card=Cardinality.ONE),
        )
        assert isinstance(expr, Expr)
        return RelFilter.wrap(parent, rel=parent, expr=expr)
    else:
        assert False, f"Unknown {syn.name}(...) combinator"  # pragma: no cover


@to_op.register
def Literal_to_op(syn: Literal, parent: Op):
    # If the parent is another expression, we just ignore it
    if isinstance(parent, Expr):
        parent = RelVoid(scope=EmptyScope(), card=Cardinality.ONE)
    return ExprConst(
        rel=parent,
        value=syn.value,
        embed=embed(syn.type),
        scope=type_scope(syn.type),
        card=parent.card,
    )


@to_op.register
def Compose_to_op(syn: Compose, parent: Op):
    return to_op(syn.b, to_op(syn.a, parent=parent))


@functools.singledispatch
def embed(v: sa.Type):
    """ Describe how to make a query out of a value."""
    raise NotImplementedError(  # pragma: no cover
        f"don't know how to embed value of type {type(v)} into query"
    )


@embed.register
def StringLiteral_embed(_: sa.String):
    return lambda v: sa.literal(v)


@embed.register
def IntegerLiteral_embed(_: sa.Integer):
    return lambda v: sa.literal(v)


@embed.register
def BooleanLiteral_embed(_: sa.Boolean):
    return lambda v: sa.literal(v)


@embed.register
def DateLiteral_embed(_: sa.Date):
    return lambda v: sa.cast(sa.literal(v.strftime("%Y-%m-%d")), sa.Date)


@embed.register
def JsonLiteral_embed(_: sa.dialects.postgresql.JSONB):
    return lambda v: sa.cast(
        sa.literal(json.dumps(v)), sa.dialects.postgresql.JSONB
    )
