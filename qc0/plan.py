"""

    qc0.plan
    ========

    Produce operations out of syntax.

"""

from __future__ import annotations

import json
import functools
import typing as ty

import sqlalchemy as sa

from .func import FuncSig, BinOpSig
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
from .syntax import (
    Syn,
    Nav,
    Compose,
    Select,
    Apply,
    BinOp,
    Literal,
    make_value,
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
    RelTake,
    RelFilter,
    RelSort,
    RelGroup,
    Expr,
    ExprRel,
    ExprAggregateRel,
    ExprRecord,
    ExprColumn,
    ExprIdentity,
    ExprConst,
    ExprApply,
    Field,
)


def make_parent(scope):
    rel = RelParent(scope=scope, card=Cardinality.ONE)
    return ExprRel.wrap(rel, rel=rel, expr=None)


def plan(syn: Syn, meta: sa.MetaData):
    """ Produce operations from syntax."""
    rel = RelVoid(
        scope=UnivScope(tables=meta.tables),
        card=Cardinality.ONE,
    )
    parent = ExprRel.wrap(rel, rel=rel, expr=None)
    return build_op(syn, parent)


def build_op(syn: Syn, parent: Op):
    op, k = norm_to_op(syn, parent=parent)
    op = k(build_op_expr(op))
    return op


def build_op_expr(op: Expr):
    if isinstance(op, ExprRel) and op.expr is None:
        if isinstance(op.scope, RecordScope):
            parent = make_parent(op.scope.scope)
            fields = {}
            for name, f in op.scope.fields.items():
                expr = build_op(f.syn, parent)
                if expr.card == Cardinality.SEQ:
                    expr = ExprAggregateRel(
                        rel=expr.rel,
                        expr=expr.expr,
                        func=None,
                        unit=embed(sa.dialects.postgresql.JSONB())([]),
                        card=Cardinality.ONE,
                        scope=EmptyScope(),
                    )
                fields[name] = Field(expr=expr, name=name)

            expr = ExprRecord.wrap(op, fields=fields)
            return ExprRel.wrap(op, rel=op.rel, expr=expr)
        if isinstance(op.scope, TableScope):
            scope = EmptyScope()
            expr = ExprIdentity.wrap(op, table=op.scope.table, scope=scope)
            return ExprRel.wrap(op, rel=op.rel, expr=expr, scope=scope)
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
            return ExprRel.wrap(op, rel=op.rel, expr=expr)
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
        rel = RelTable(
            table=table,
            scope=TableScope(table=table),
            card=Cardinality.SEQ,
        )
        return ExprRel.wrap(rel, rel=rel, expr=None)

    elif isinstance(parent.scope, TableScope):
        table = parent.scope.table

        if syn.name in table.columns:
            column = table.columns[syn.name]
            next_scope = type_scope(column.type)
            next_card = parent.card * Cardinality.ONE
            return parent.replace_expr(
                expr=ExprColumn(
                    column=column,
                    scope=next_scope,
                    card=next_card,
                )
            )

        fk = parent.scope.foreign_keys.get(syn.name)
        if fk:
            assert parent.expr is None, parent.expr
            rel = RelJoin(
                rel=parent.rel,
                fk=fk,
                scope=TableScope(table=fk.column.table),
                card=parent.card * Cardinality.ONE,
            )
            return parent.replace_rel(rel, expr=None)

        fk = parent.scope.rev_foreign_keys.get(syn.name)
        if fk:
            assert parent.expr is None, parent.expr
            rel = RelRevJoin(
                rel=parent.rel,
                fk=fk,
                scope=TableScope(table=fk.parent.table),
                card=parent.card * Cardinality.SEQ,
            )
            return parent.replace_rel(rel, expr=None)

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
                assert isinstance(parent.rel, RelGroup)
                return ExprRel.wrap(
                    parent.rel.rel, rel=parent.rel.rel, expr=None
                )

            def wrap(expr):
                if not isinstance(expr, ExprAggregateRel):
                    expr = ExprAggregateRel(
                        rel=expr.rel,
                        expr=expr.expr,
                        func=None,
                        unit=embed(sa.dialects.postgresql.JSONB())([]),
                        scope=expr.scope,
                        card=parent.card * Cardinality.ONE,
                    )
                name = parent.scope.add_aggregate(expr)
                expr = ExprColumn.wrap(
                    expr,
                    column=sa.column(name),
                    card=parent.card * Cardinality.ONE,
                )
                return parent.replace_expr(expr)

            rel = RelAggregateParent(
                scope=parent.scope.scope,
                card=Cardinality.SEQ,
            )

            return (
                ExprRel.wrap(rel, rel=rel, expr=None),
                wrap,
            )
        if syn.name in parent.scope.fields:
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
            return parent.replace_expr(expr=expr)
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
            expr=parent.expr,
            rel=parent.rel,
            func=syn.name,
            unit=unit_by_name[syn.name],
            scope=EmptyScope(),
            card=Cardinality.ONE,
        )
    elif syn.name == "take":
        assert len(syn.args) == 1, "take(...): expected a single argument"
        take = syn.args[0]
        # TODO(andreypopp): this shouldn't be the parent really...
        take = run_to_op(take, make_parent(parent.scope))
        assert (
            parent.card >= Cardinality.SEQ
        ), f"{syn.name}(...): expected a sequence of items"
        rel = RelTake.wrap(parent.rel, rel=parent.rel, take=take)
        return parent.replace(rel=rel)
    elif syn.name == "first":
        assert len(syn.args) == 0, "first(): expected no arguments"
        assert parent.card >= Cardinality.SEQ, f"{syn.name}(): plural req"
        take = run_to_op(make_value(1), make_parent(parent.scope))
        rel = RelTake.wrap(
            parent.rel,
            rel=parent.rel,
            take=take,
            card=Cardinality.ONE,
        )
        return parent.replace(rel=rel, card=rel.card)
    elif syn.name == "filter":
        assert len(syn.args) == 1, "filter(...): expected a single argument"
        expr = syn.args[0]
        assert (
            parent.card >= Cardinality.SEQ
        ), f"{syn.name}(...): expected a sequence of items"
        expr = run_to_op(expr, make_parent(parent.scope))
        rel = RelFilter.wrap(parent.rel, rel=parent.rel, expr=expr)
        return parent.replace(rel=rel)
    elif syn.name == "sort":
        assert parent.card >= Cardinality.SEQ, f"{syn.name}(): plural req"
        args = [run_to_op(arg, make_parent(parent.scope)) for arg in syn.args]
        rel = RelSort.wrap(parent.rel, rel=parent.rel, args=args)
        return parent.replace(rel=rel)
    elif syn.name == "group":
        assert (
            parent.card >= Cardinality.SEQ
        ), f"{syn.name}(...): expected a sequence of items"
        # TODO(andreypopp): fix usage of syn.args here
        scope = GroupScope(scope=parent.scope, fields=syn.args, aggregates={})
        fields = {}
        for name, f in syn.args.items():
            expr = run_to_op(f.syn, make_parent(parent.scope))
            if isinstance(expr, Rel):
                if expr.card == Cardinality.SEQ:
                    assert False, "group(..): unable to group by a sequence"
                expr = ExprRel.wrap(expr, rel=expr)
            fields[name] = Field(expr=expr, name=name)

        rel = RelGroup(
            rel=parent.rel,
            fields=fields,
            scope=scope,
            aggregates=scope.aggregates,
            card=Cardinality.SEQ,
        )
        return parent.replace(rel=rel, scope=rel.scope, card=rel.card)
    else:
        sig = FuncSig.get(syn.name)
        assert sig, f"unknown query combinator {syn.name}()"
        args = []
        for arg in syn.args:
            arg = run_to_op(arg, make_parent(parent.scope))
            if isinstance(arg, Rel):
                arg = ExprRel.wrap(arg, rel=arg)
            args.append(arg)
        sig.validate(args)

        expr = ExprApply.wrap(
            parent,
            expr=parent.expr,
            compile=sig.compile,
            args=args,
            card=functools.reduce(
                lambda card, arg: card * arg.card,
                args,
                parent.card,
            ),
        )
        return parent.replace_expr(expr)


@to_op.register
def BinOp_to_op(syn: BinOp, parent: Op):
    assert isinstance(parent, ExprRel)

    def make(a, b):
        sig.validate((a, b))
        return ExprApply.wrap(
            parent,
            expr=None,
            compile=lambda parent, args: sig.compile(args[0], args[1]),
            args=(a, b),
            card=parent.card * a.card * b.card,
        )

    sig = BinOpSig.get(syn.op)
    assert sig, f"unknown query combinator {syn.name}()"
    a, ak = norm_to_op(syn.a, make_parent(parent.scope))
    a = build_op_expr(a)
    b, bk = norm_to_op(syn.b, make_parent(parent.scope))
    b = build_op_expr(b)

    if a.card > b.card:
        expr = make(a.expr, bk(b))
        expr = ak(a.replace(expr=expr))
    elif a.card < b.card:
        expr = make(ak(a), b.expr)
        expr = bk(b.replace(expr=expr))
    else:
        a = ak(a)
        b = bk(b)
        expr = make(a, b)
    return parent.replace_expr(expr)


@to_op.register
def Literal_to_op(syn: Literal, parent: Op):
    assert isinstance(parent, ExprRel), parent
    expr = ExprConst(
        value=syn.value,
        embed=embed(syn.type),
        scope=type_scope(syn.type),
        card=parent.card,
    )
    return parent.replace_expr(expr)


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
