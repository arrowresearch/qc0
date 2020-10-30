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

from .func import FuncSig, BinOpSig, AggrSig, JsonAggSig
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
    Desc,
    make_value,
)
from .op import (
    Op,
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
    ExprOp,
    ExprOpAggregate,
    ExprRecord,
    ExprColumn,
    ExprIdentity,
    ExprConst,
    ExprApply,
    Field,
    Sort,
)


def make_parent(parent):
    assert isinstance(parent, Op)
    if isinstance(parent.rel, RelParent) and parent.expr is None:
        return parent
    return Op(
        rel=RelParent(parent=parent),
        expr=None,
        scope=parent.scope,
        card=Cardinality.ONE,
    )


def plan(syn: Syn, meta: sa.MetaData):
    """ Produce operations from syntax."""
    parent = Op(
        rel=RelVoid(),
        expr=None,
        card=Cardinality.ONE,
        scope=UnivScope(tables=meta.tables),
    )
    return build_op(syn, parent)


def build_op(syn: Syn, parent: Op):
    op, k = norm_to_op(syn, parent=parent)
    op = k(build_op_expr(op))
    return op


def build_op_expr(op: Op):
    if op.expr is None:
        if isinstance(op.scope, RecordScope):
            fields = {}
            for name, f in op.scope.fields.items():
                fop = build_op(f.syn, make_parent(op.scope.parent))
                if fop.card == Cardinality.SEQ:
                    expr = ExprOpAggregate(op=fop, sig=JsonAggSig)
                else:
                    expr = ExprOp(op=fop)
                fields[name] = Field(expr=expr, name=name)
            return Op(
                rel=op.rel,
                expr=ExprRecord(fields=fields),
                card=op.card,
                scope=op.scope,
            )
        if isinstance(op.scope, TableScope):
            expr = ExprIdentity(table=op.scope.table)
            return Op(
                rel=op.rel,
                expr=ExprIdentity(table=op.scope.table),
                scope=EmptyScope(),
                card=op.card,
            )
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
                    expr=ExprOp(
                        Op(
                            expr=ExprColumn(column=sa.column(name)),
                            rel=RelParent(parent=op),
                            card=Cardinality.ONE,
                            scope=EmptyScope(),
                        )
                    ),
                )
            expr = ExprRecord(fields=fields)
            return Op(rel=op.rel, expr=expr, card=op.card, scope=op.scope)
        assert False, f"unable to build an expr at this scope: {op!r}"
    return op


@functools.singledispatch
def to_op(syn: ty.Optional[Syn], parent: Op):
    """ Produce an operation out of a query."""
    raise NotImplementedError(type(syn))  # pragma: no cover


def run_to_op(syn, parent):
    op = to_op(syn, parent)
    if isinstance(op, tuple):
        op, k = op
        op = k(op)
    assert isinstance(op, Op), type(op)
    return op


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
        return Op(
            rel=RelTable(table=table),
            expr=None,
            card=Cardinality.SEQ,
            scope=TableScope(table=table),
        )

    elif isinstance(parent.scope, TableScope):
        table = parent.scope.table

        if syn.name in table.columns:
            column = table.columns[syn.name]
            next_scope = type_scope(column.type)
            return parent.grow_expr(
                scope=next_scope,
                expr=ExprColumn(column=column),
            )

        fk = parent.scope.foreign_keys.get(syn.name)
        if fk:
            assert parent.expr is None, parent.expr
            return parent.grow_rel(
                rel=RelJoin(rel=parent.rel, fk=fk),
                scope=TableScope(table=fk.column.table),
            )

        fk = parent.scope.rev_foreign_keys.get(syn.name)
        if fk:
            assert parent.expr is None, parent.expr
            return parent.grow_rel(
                rel=RelRevJoin(rel=parent.rel, fk=fk),
                scope=TableScope(table=fk.parent.table),
                card=Cardinality.SEQ,
            )

        assert False, f"Unable to lookup {syn.name}"  # pragma: no cover

    elif isinstance(parent.scope, RecordScope):
        if syn.name in parent.scope.fields:
            field = parent.scope.fields[syn.name]
            return run_to_op(
                field.syn,
                parent=parent.replace(scope=parent.scope.parent.scope),
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
                return Op(
                    rel=parent.rel.rel,
                    expr=None,
                    card=Cardinality.SEQ,
                    scope=parent.scope.scope,
                )

            def wrap(op):
                if op.card == Cardinality.SEQ:
                    expr = ExprOpAggregate(op=op, sig=JsonAggSig)
                else:
                    assert isinstance(op.rel, RelVoid)
                    assert isinstance(op.expr, ExprOpAggregate)
                    expr = op.expr
                name = parent.scope.add_aggregate(expr)
                return parent.grow_expr(
                    expr=ExprColumn(column=sa.column(name)),
                    scope=EmptyScope(),
                )

            rel = RelAggregateParent()

            return (
                Op(
                    rel=rel,
                    expr=None,
                    scope=parent.scope.scope,
                    card=Cardinality.SEQ,
                ),
                wrap,
            )
        if syn.name in parent.scope.fields:
            field = parent.scope.fields[syn.name]
            expr = ExprColumn(column=sa.column(syn.name))
            return parent.grow_expr(expr=expr, scope=EmptyScope())
        else:
            names = ", ".join(parent.scope.fields)  # pragma: no cover
            assert (  # pragma: no cover
                False
            ), f"Unable to lookup {syn.name} in record scope, names: {names}"

    elif isinstance(parent.scope, SyntheticScope):
        transform, type = parent.scope.lookup(syn.name)
        next_scope = type_scope(type)
        assert transform is not None, f"Unable to lookup {syn.name}"
        expr = ExprApply(expr=ExprOp(parent), args=(), compile=transform)
        return parent.grow_expr(expr=expr, scope=next_scope)

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
    scope = RecordScope(parent=parent, fields=syn.fields)
    return parent.replace(scope=scope)


@to_op.register
def Apply_to_op(syn: Apply, parent: Op):
    if syn.name == "fork":
        assert len(syn.args) == 0, "fork(...): takes no arguments"
        while isinstance(parent.rel, RelParent):
            parent = parent.rel.parent
        return parent
    elif syn.name == "take":
        assert len(syn.args) == 1, "take(...): expected a single argument"
        take = syn.args[0]
        # TODO(andreypopp): this shouldn't be the parent really...
        take = run_to_op(take, make_parent(parent))
        assert parent.card >= Cardinality.SEQ
        assert take.card == Cardinality.ONE
        rel = RelTake(rel=parent.rel, take=ExprOp(take))
        return parent.grow_rel(rel=rel)
    elif syn.name == "first":
        assert len(syn.args) == 0, "first(): expected no arguments"
        take = run_to_op(make_value(1), make_parent(parent))
        assert parent.card >= Cardinality.SEQ
        assert take.card >= Cardinality.ONE
        rel = RelTake(rel=parent.rel, take=ExprOp(take))
        return parent.grow_rel(rel=rel, card=Cardinality.ONE)
    elif syn.name == "filter":
        assert len(syn.args) == 1, "filter(...): expected a single argument"
        expr = syn.args[0]
        assert (
            parent.card >= Cardinality.SEQ
        ), f"{syn.name}(...): expected a sequence of items"
        expr = run_to_op(expr, make_parent(parent))
        rel = RelFilter(rel=parent.rel, expr=ExprOp(expr))
        return parent.grow_rel(rel=rel)
    elif syn.name == "sort":
        assert parent.card >= Cardinality.SEQ, f"{syn.name}(): plural req"
        sort = []
        for arg in syn.args:
            arg, desc = (
                (arg.syn, True) if isinstance(arg, Desc) else (arg, False)
            )
            arg = run_to_op(arg, make_parent(parent))
            assert arg.card == Cardinality.ONE
            sort.append(Sort(expr=ExprOp(arg), desc=desc))
        rel = RelSort(rel=parent.rel, sort=sort)
        return parent.grow_rel(rel=rel)
    elif syn.name == "group":
        assert (
            parent.card >= Cardinality.SEQ
        ), f"{syn.name}(...): expected a sequence of items"
        # TODO(andreypopp): fix usage of syn.args here
        scope = GroupScope(scope=parent.scope, fields=syn.args, aggregates={})
        fields = {}
        for name, f in syn.args.items():
            expr = run_to_op(f.syn, make_parent(parent))
            fields[name] = Field(expr=ExprOp(expr), name=name)

        rel = RelGroup(
            rel=parent.rel,
            fields=fields,
            aggregates=scope.aggregates,
        )
        return parent.grow_rel(rel=rel, scope=scope)
    else:

        sig = AggrSig.get(syn.name)
        if sig:
            assert parent.card >= Cardinality.SEQ
            return Op(
                expr=ExprOpAggregate(op=parent, sig=sig),
                rel=RelVoid(),
                scope=EmptyScope(),
                card=Cardinality.ONE,
            )

        sig = FuncSig.get(syn.name)
        if sig:
            args = []
            for arg in syn.args:
                arg = run_to_op(arg, make_parent(parent))
                assert arg.card == Cardinality.ONE
                args.append(ExprOp(arg))
            sig.validate(args)

            expr = ExprApply(
                expr=parent.expr,
                compile=sig.compile,
                args=args,
            )
            return parent.grow_expr(expr)

        assert sig, f"unknown query combinator {syn.name}()"


@to_op.register
def BinOp_to_op(syn: BinOp, parent: Op):
    assert isinstance(parent, Op)
    sig = BinOpSig.get(syn.op)
    assert sig, f"unknown query combinator {syn.name}()"

    def make(a: Expr, b: Expr):
        if isinstance(a, Op):
            a = ExprOp(a)
        if isinstance(b, Op):
            b = ExprOp(b)
        sig.validate((a, b))
        expr = ExprApply(
            expr=None,
            compile=lambda parent, args: sig.compile(args[0], args[1]),
            args=(a, b),
        )
        return expr

    a, ak = norm_to_op(syn.a, make_parent(parent))
    a = build_op_expr(a)
    b, bk = norm_to_op(syn.b, make_parent(parent))
    b = build_op_expr(b)

    if a.card > b.card:
        expr = make(a.expr, bk(b))
        a = ak(a.grow_expr(expr=expr))
        expr = ExprOp(a)
    elif a.card < b.card:
        expr = make(ak(a), b.expr)
        b = bk(b.grow_expr(expr=expr))
        expr = ExprOp(b)
    else:
        a = ak(a)
        b = bk(b)
        expr = make(a, b)
    card = parent.card * a.card * b.card
    return parent.grow_expr(expr, scope=EmptyScope(), card=card)


@to_op.register
def Literal_to_op(syn: Literal, parent: Op):
    assert isinstance(parent, Op), parent
    expr = ExprConst(value=syn.value, embed=embed(syn.type))
    return parent.grow_expr(expr, scope=type_scope(syn.type))


@to_op.register
def Compose_to_op(syn: Compose, parent: Op):
    a, ak = norm_to_op(syn.a, parent)
    b, bk = norm_to_op(syn.b, a)
    return b, lambda op: bk(ak(op))


@to_op.register
def Desc_to_op(syn: Desc, parent: Op):
    assert False, "desc() is only valid inside sort(..)"


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
def FloatLiteral_embed(_: sa.Float):
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
