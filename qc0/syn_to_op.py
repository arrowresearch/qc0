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

from .func import FuncSig
from .scope import (
    Cardinality,
    EmptyScope,
    UnivScope,
    TableScope,
    RecordScope,
    GroupScope,
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
    RelAggregateParent,
    RelExpr,
    RelTake,
    RelFilter,
    RelGroup,
    Expr,
    ExprRel,
    ExprAggregateRel,
    ExprRecord,
    ExprColumn,
    ExprIdentity,
    ExprConst,
    ExprBinOp,
    ExprApply,
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
    op, k = norm_to_op(syn, parent=parent)
    op = k(build_op_expr(op))
    return op


def build_op_expr(op: Op):
    if not (isinstance(op, RelExpr) or isinstance(op, Expr)):
        if isinstance(op.scope, RecordScope):
            parent = RelParent(
                scope=op.scope.scope,
                card=Cardinality.ONE,
            )
            fields = {}
            for name, f in op.scope.fields.items():
                expr = build_op(f.syn, parent)
                if isinstance(expr, Rel):
                    if expr.card == Cardinality.SEQ:
                        expr = ExprAggregateRel(
                            rel=expr,
                            func=None,
                            unit=embed(sa.dialects.postgresql.JSONB())([]),
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
        if isinstance(op.scope, GroupScope):
            # From GroupScope we select
            # relation were grouped by:
            #
            #   relation.group(x: ..., y: ...)
            #
            # becomes:
            #
            #   relation.group(x: ..., y: ...) {x, y}
            #
            fields = {}
            for name, f in op.scope.fields.items():
                fields[name] = Field(
                    name=name,
                    expr=ExprColumn(
                        column=sa.column(name),
                        scope=EmptyScope(),
                        card=Cardinality.ONE,
                    ),
                )
            expr = ExprRecord.wrap(op, fields=fields)
            return RelExpr.wrap(op, rel=op, expr=expr)
        assert False, f"unable to build an expr at this scope: {op!r}"
    return op


@functools.singledispatch
def to_op(syn: ty.Optional[Syn], parent: Op):
    """ Produce an operation out of a query."""
    raise NotImplementedError(type(syn))  # pragma: no cover


def run_to_op(syn, parent):
    res = to_op(syn, parent)
    if isinstance(res, tuple):
        op, k = res
        return k(op)
    else:
        return res


def norm_to_op(syn, parent):
    res = to_op(syn, parent)
    if isinstance(res, tuple):
        return res
    else:
        return res, lambda op: op


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
            return run_to_op(
                field.syn, parent=parent.replace(scope=parent.scope.scope)
            )
        else:
            names = ", ".join(parent.scope.fields)  # pragma: no cover
            assert (  # pragma: no cover
                False
            ), f"Unable to lookup {syn.name} in record scope, names: {names}"

    elif isinstance(parent.scope, GroupScope):
        if syn.name == "_":
            if parent.card == Cardinality.SEQ:
                assert isinstance(parent, RelGroup)
                return parent.rel

            def wrap(expr):
                if not isinstance(expr, ExprAggregateRel):
                    expr = ExprAggregateRel(
                        rel=expr,
                        func=None,
                        unit=embed(sa.dialects.postgresql.JSONB())([]),
                        scope=expr.scope,
                        card=parent.card * Cardinality.ONE,
                    )
                name = parent.scope.add_aggregate(expr)
                expr = ExprColumn.wrap(expr, column=sa.column(name))
                return RelExpr.wrap(
                    expr,
                    rel=parent,
                    expr=expr,
                    card=parent.card * Cardinality.ONE,
                )

            return (
                RelAggregateParent(
                    scope=parent.scope.scope,
                    card=Cardinality.SEQ,
                ),
                wrap,
            )
        if syn.name in parent.scope.fields:
            assert isinstance(parent, Rel)
            field = parent.scope.fields[syn.name]
            # TODO(andreypopp): determine scope by
            # column type here
            scope = EmptyScope()
            card = parent.card * Cardinality.ONE
            expr = ExprColumn(
                scope=scope,
                card=card,
                column=sa.column(syn.name),
            )
            return RelExpr.wrap(expr, rel=parent, expr=expr)
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
        return ExprApply.wrap(
            parent,
            expr=parent,
            args=(),
            compile=transform,
            scope=next_scope,
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
    # See build_op_expr where we create ExprRecord instead for the selects
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
        ), f"{syn.name}(...): expected a sequence of items"
        # TODO(andreypopp): need to introduce proper function sigs
        unit_by_name = {
            "count": 0,
            "exists": False,
            "sum": 0,
        }
        return ExprAggregateRel.wrap(
            parent,
            rel=parent,
            func=syn.name,
            unit=unit_by_name[syn.name],
            scope=EmptyScope(),
            card=Cardinality.ONE,
        )
    elif syn.name == "take":
        assert len(syn.args) == 1, "take(...): expected a single argument"
        take = syn.args[0]
        # TODO(andreypopp): this shouldn't be the parent really...
        take = run_to_op(take, RelParent.wrap(parent))
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
        a = run_to_op(a, parent)
        if isinstance(a, Rel):
            a = ExprRel.wrap(a, rel=a)
        b = run_to_op(b, parent)
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
        expr = run_to_op(
            expr,
            RelParent(scope=parent.scope, card=Cardinality.ONE),
        )
        if not isinstance(expr, Expr):
            expr = ExprRel.wrap(expr, rel=expr)
        assert isinstance(expr, Expr)
        return RelFilter.wrap(parent, rel=parent, expr=expr)
    elif syn.name == "group":
        assert isinstance(
            parent, Rel
        ), f"group(...): requires a rel, got {type(parent)}"
        assert (
            parent.card == Cardinality.SEQ
        ), "group(...): requires a plural rel"
        # TODO(andreypopp): fix usage of syn.args here
        scope = GroupScope(scope=parent.scope, fields=syn.args, aggregates={})
        field_parent = RelParent(
            scope=parent.scope,
            card=Cardinality.ONE,
        )
        fields = {}
        for name, f in syn.args.items():
            expr = run_to_op(f.syn, field_parent)
            if isinstance(expr, Rel):
                if expr.card == Cardinality.SEQ:
                    assert False, "group(..): unable to group by a sequence"
                expr = ExprRel.wrap(expr, rel=expr)
            fields[name] = Field(expr=expr, name=name)

        return RelGroup(
            rel=parent,
            fields=fields,
            scope=scope,
            aggregates=scope.aggregates,
            card=Cardinality.SEQ,
        )
    else:
        sig = FuncSig.get(syn.name)
        assert sig, f"unknown query combinator {syn.name}()"
        args = []
        for arg in syn.args:
            arg = run_to_op(arg, RelParent.wrap(parent, card=Cardinality.ONE))
            args.append(arg)
        sig.validate(args)

        make = lambda parent: ExprApply.wrap(
            parent,
            expr=parent,
            compile=sig.compile,
            args=args,
        )

        if isinstance(parent, Expr):
            return make(parent)
        elif isinstance(parent, RelExpr):
            return parent.replace(expr=make(parent.expr))
        else:
            # TODO(andreypopp): this needs to be fixed... one idea is to rewrite
            # the closest RelExpr by wrapping it with make (see above where it
            # is done for the immediate RelExpr)
            assert False, type(parent)


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
    a, ak = norm_to_op(syn.a, parent)
    b, bk = norm_to_op(syn.b, a)
    return b, lambda op: bk(ak(op))


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
