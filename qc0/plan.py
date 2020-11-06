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
import sqlalchemy.dialects.postgresql as sa_pg

from .sig import (
    Sig,
    FuncSig,
    BinOpSig,
    AggrSig,
    JsonAggSig,
    FilterSig,
    AroundSig,
    TakeSig,
    FirstSig,
    SortSig,
    GroupSig,
    SelectSig,
)
from .scope import (
    Cardinality,
    Scope,
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
    RelAroundParent,
    Expr,
    ExprOp,
    ExprRecord,
    ExprColumn,
    ExprIdentity,
    ExprConst,
    ExprApply,
    Field,
    Sort,
)


def plan(syn: Syn, meta: sa.MetaData) -> Op:
    """ Produce operations from syntax."""
    parent = Op(
        rel=RelVoid(),
        expr=None,
        card=Cardinality.ONE,
        scope=UnivScope(tables=meta.tables),
        syn=None,
    )
    return build_op(syn, parent)


def build_op(syn: Syn, parent: Op) -> Op:
    op, k = norm_to_op(syn, parent=parent)
    op = k(build_op_expr(op))
    return op


def build_op_expr(op: Op) -> Op:
    if op.expr is None:
        if isinstance(op.scope, RecordScope):
            syn = Apply("select", op.scope.fields)
            fields = {}
            for f in syn.args.values():
                field_op = build_op(
                    f.syn,
                    make_parent(op.replace(scope=op.scope.parent)),
                )
                if field_op.card == Cardinality.SEQ:
                    field_op = field_op.aggregate(JsonAggSig())
                fields[f.name] = Field(op=field_op, name=f.name)
            expr = ExprRecord(fields=fields)
            return Op(
                rel=op.rel,
                expr=expr,
                card=op.card,
                scope=op.scope,
                syn=syn,
            )
        if isinstance(op.scope, TableScope):
            expr = ExprIdentity(table=op.scope.table)
            return Op(
                rel=op.rel,
                expr=ExprIdentity(table=op.scope.table),
                scope=EmptyScope(),
                card=op.card,
                syn=op.syn,
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
                    op=Op(
                        expr=ExprColumn(column=sa.column(name)),
                        rel=RelParent(parent=op),
                        card=Cardinality.ONE,
                        scope=EmptyScope(),
                        syn=Nav(name),
                    ),
                )
            expr = ExprRecord(fields=fields)
            return Op(
                rel=op.rel,
                expr=expr,
                card=op.card,
                scope=op.scope,
                syn=None,
            )
    return op


def make_parent(parent: Op) -> Op:
    assert isinstance(parent, Op)
    if isinstance(parent.rel, RelParent) and parent.expr is None:
        return parent
    return Op(
        rel=RelParent(parent=parent),
        expr=None,
        scope=parent.scope,
        card=Cardinality.ONE,
        syn=None,
    )


#
# Produce operation out of syntax
#


@functools.singledispatch
def to_op(syn: ty.Optional[Syn], parent: Op) -> Op:
    """ Produce an operation out of a query."""
    raise NotImplementedError(type(syn))  # pragma: no cover


def run_to_op(syn, parent):
    op = to_op(syn, parent)
    return op[1](op[0]) if isinstance(op, tuple) else op


def norm_to_op(syn, parent):
    res = to_op(syn, parent)
    return res if isinstance(res, tuple) else (res, lambda op: op)


@to_op.register
def None_to_op(syn: type(None), parent: Op):
    return parent


@to_op.register
def Nav_to_op(syn: Nav, parent: Op):
    return navigate(parent.scope, syn, parent)


@to_op.register
def Apply_to_op(syn: Apply, parent: Op):
    sig = Sig.get(syn.name)
    assert sig, f"unknown query combinator {syn.name}()"
    return sig_to_op(sig(), syn, parent)


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
        a = ak(a.grow_expr(expr=expr, syn=syn.a))
        expr = ExprOp(a)
    elif a.card < b.card:
        expr = make(ak(a), b.expr)
        b = bk(b.grow_expr(expr=expr, syn=syn.b))
        expr = ExprOp(b)
    else:
        a = ak(a)
        b = bk(b)
        expr = make(a, b)
    card = parent.card * a.card * b.card
    return parent.grow_expr(expr, scope=EmptyScope(), card=card, syn=syn)


@to_op.register
def Literal_to_op(syn: Literal, parent: Op):
    assert isinstance(parent, Op), parent
    expr = ExprConst(value=syn.value, embed=embed(syn.type))
    return parent.grow_expr(expr, scope=type_scope(syn.type), syn=syn)


@to_op.register
def Compose_to_op(syn: Compose, parent: Op):
    a, ak = norm_to_op(syn.a, parent)
    b, bk = norm_to_op(syn.b, a)
    return b, lambda op: bk(ak(op))


@to_op.register
def Desc_to_op(syn: Desc, parent: Op):
    assert False, "desc() is only valid inside sort(..)"


#
# Navigation
#


@functools.singledispatch
def navigate(scope: Scope, syn: Nav, parent: Op):
    raise NotImplementedError(type(syn))  # pragma: no cover


@navigate.register
def UnivScope_navigate(scope: UnivScope, syn: Nav, parent: Op):
    table = parent.scope.tables[syn.name]
    rel = RelTable(table=table)
    scope = TableScope(rel=rel, table=table)
    return Op(
        rel=rel,
        expr=None,
        card=Cardinality.SEQ,
        scope=scope,
        syn=syn,
    )


@navigate.register
def TableScope_navigate(scope: TableScope, syn: Nav, parent: Op):
    table = parent.scope.table

    if syn.name in table.columns:
        column = table.columns[syn.name]
        next_scope = type_scope(column.type)
        return parent.grow_expr(
            scope=next_scope,
            expr=ExprColumn(column=column),
            syn=syn,
        )

    fk = parent.scope.foreign_keys.get(syn.name)
    if fk:
        assert parent.expr is None, parent.expr
        rel = RelJoin(rel=parent.rel, fk=fk)
        scope = TableScope(rel=rel, table=fk.column.table)
        return parent.grow_rel(
            rel=rel,
            scope=scope,
            syn=syn,
        )

    fk = parent.scope.rev_foreign_keys.get(syn.name)
    if fk:
        assert parent.expr is None, parent.expr
        rel = RelRevJoin(rel=parent.rel, fk=fk)
        scope = TableScope(rel=rel, table=fk.parent.table)
        return parent.grow_rel(
            rel=rel,
            scope=scope,
            card=Cardinality.SEQ,
            syn=syn,
        )

    assert (  # pragma: no cover
        False
    ), f"Unable to lookup {syn.name} in {parent.scope.__class__.__name__}"


@navigate.register
def RecordScope_navigate(scope: RecordScope, syn: Nav, parent: Op):
    if syn.name in parent.scope.fields:
        field = parent.scope.fields[syn.name]
        op = run_to_op(
            field.syn,
            parent=make_parent(parent.replace(scope=parent.scope.parent)),
        )
        if op.expr or op.sig:
            return parent.grow_expr(
                expr=ExprOp(op),
                scope=op.scope,
                card=op.card * parent.card,
            )
        else:
            assert parent.expr is None and parent.sig is None

            def rebase(rel, base):
                if isinstance(rel, RelParent):
                    return base
                else:
                    return rel.replace(rel=rebase(rel.rel, base))

            return parent.grow_rel(
                rel=rebase(op.rel, parent.rel),
                card=op.card * parent.card,
                scope=op.scope,
            )
    else:
        names = ", ".join(parent.scope.fields)  # pragma: no cover
        assert (  # pragma: no cover
            False
        ), f"Unable to lookup {syn.name} in record scope, names: {names}"


@navigate.register
def GroupScope_navigate(scope: GroupScope, syn: Nav, parent: Op):
    if syn.name == "_":
        if parent.card == Cardinality.SEQ:
            while isinstance(parent.rel, RelParent):
                parent = parent.rel.parent
            assert isinstance(parent.rel, RelGroup), parent
            return Op(
                rel=parent.rel.rel,
                expr=None,
                card=Cardinality.SEQ,
                scope=parent.scope.scope,
                syn=syn,
            )

        def wrap(op):
            if op.card == Cardinality.SEQ:
                op = op.aggregate(JsonAggSig())
            else:
                assert op.sig is not None
            idx = len(parent.scope.rel.compute)
            name = f"compute_{idx}"
            parent.scope.rel.compute.append(Field(name=name, op=op))
            return parent.grow_expr(
                expr=ExprColumn(column=sa.column(name)),
                scope=EmptyScope(),
                syn=syn,
            )

        rel = RelAggregateParent()

        return (
            Op(
                rel=rel,
                expr=None,
                scope=parent.scope.scope,
                card=Cardinality.SEQ,
                syn=syn,
            ),
            wrap,
        )

    if syn.name in scope.fields:
        assert parent.expr is None
        expr = ExprColumn(column=sa.column(syn.name))
        return parent.grow_expr(expr=expr, scope=EmptyScope(), syn=syn)
    else:
        names = ", ".join(parent.scope.fields)  # pragma: no cover
        assert (  # pragma: no cover
            False
        ), f"Unable to lookup {syn.name} in record scope, names: {names}"


@navigate.register
def SyntheticScope_navigate(scope: SyntheticScope, syn: Nav, parent: Op):
    transform, type = parent.scope.lookup(syn.name)
    next_scope = type_scope(type)
    assert transform is not None, f"Unable to lookup {syn.name}"
    expr = ExprApply(expr=parent.expr, args=(), compile=transform)
    return parent.grow_expr(expr=expr, scope=next_scope, syn=syn)


@navigate.register
def EmptyScope_navigate(scope: EmptyScope, syn: Nav, parent: Op):
    assert (  # pragma: no cover
        False
    ), f"Unable to lookup {syn.name} in empty scope"


#
# Application of signatures (built-in combinators, functions, aggregates)
#


@functools.singledispatch
def sig_to_op(sig: Sig, syn: Apply, parent: Op):
    raise NotImplementedError(type(sig))  # pragma: no cover


@sig_to_op.register
def Select_to_op(sig: SelectSig, syn: Apply, parent: Op):
    # In general select() results in ExprRecord but we don't create it here as
    # the next syntax might make such op useless, consider the folloing cases:
    #
    #     region{name: name}.name
    #     region{name: name}{region_name: name}
    #
    # See build_op_expr where we create ExprRecord instead for the selects
    # which are "final".
    scope = RecordScope(parent=parent.scope, fields=syn.args)
    return parent.replace(scope=scope)


@sig_to_op.register
def AroundSig_to_op(sig: AroundSig, syn: Syn, parent: Op):
    assert len(syn.args) <= 1
    through = syn.args[0] if syn.args else None
    if isinstance(parent.rel, RelParent):
        syn = parent.rel.parent.syn
    else:
        syn = parent.syn
    if through:
        on = run_to_op(through, parent.replace(rel=RelAroundParent()))
        return run_to_op(syn, on)
    else:
        return run_to_op(parent.syn, parent.replace(card=Cardinality.SEQ))


@sig_to_op.register
def TakeSig_to_op(sig: TakeSig, syn: Syn, parent: Op):
    assert len(syn.args) == 1, "take(...): expected a single argument"
    take = syn.args[0]
    # TODO(andreypopp): this shouldn't be the parent really...
    take = run_to_op(take, make_parent(parent))
    assert parent.card >= Cardinality.SEQ
    assert take.card == Cardinality.ONE
    rel = RelTake(rel=parent.rel, take=ExprOp(take))
    return parent.grow_rel(rel=rel, syn=syn)


@sig_to_op.register
def FirstSig_to_op(sig: FirstSig, syn: Syn, parent: Op):
    assert len(syn.args) == 0, "first(): expected no arguments"
    take = run_to_op(make_value(1), make_parent(parent))
    assert parent.card >= Cardinality.SEQ
    assert take.card >= Cardinality.ONE
    rel = RelTake(rel=parent.rel, take=ExprOp(take))
    return parent.grow_rel(rel=rel, syn=syn, card=Cardinality.ONE)


@sig_to_op.register
def FilterSig_to_op(sig: FilterSig, syn: Syn, parent: Op):
    assert len(syn.args) == 1, "filter(...): expected a single argument"
    expr = syn.args[0]
    assert (
        parent.card >= Cardinality.SEQ
    ), f"{syn.name}(...): expected a sequence of items"
    expr = run_to_op(expr, make_parent(parent))
    rel = RelFilter(rel=parent.rel, expr=ExprOp(expr))
    return parent.grow_rel(rel=rel, syn=syn)


@sig_to_op.register
def SortSig_to_op(sig: SortSig, syn: Syn, parent: Op):
    assert parent.card >= Cardinality.SEQ, f"{syn.name}(): plural req"
    sort = []
    for arg in syn.args:
        arg, desc = (arg.syn, True) if isinstance(arg, Desc) else (arg, False)
        arg = run_to_op(arg, make_parent(parent))
        assert arg.card == Cardinality.ONE
        sort.append(Sort(expr=ExprOp(arg), desc=desc))
    rel = RelSort(rel=parent.rel, sort=sort)
    return parent.grow_rel(rel=rel, syn=syn)


@sig_to_op.register
def GroupSig_to_op(sig: GroupSig, syn: Syn, parent: Op):
    assert (
        parent.card >= Cardinality.SEQ
    ), f"{syn.name}(...): expected a sequence of items"
    # TODO(andreypopp): fix usage of syn.args here
    fields = {}
    for name, f in syn.args.items():
        op = run_to_op(f.syn, make_parent(parent))
        if op.expr is None:
            if isinstance(op.scope, TableScope):
                op = op.grow_expr(ExprIdentity(table=op.scope.table))
        fields[name] = Field(op=op, name=name)

    rel = RelGroup(rel=parent.rel, fields=fields, compute=[])
    scope = GroupScope(scope=parent.scope, fields=syn.args, rel=rel)
    card = Cardinality.SEQ if fields else Cardinality.ONE
    return parent.grow_rel(rel=rel, syn=syn, card=card, scope=scope)


@sig_to_op.register
def FuncSig_to_op(sig: FuncSig, syn: Apply, parent: Op):
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
    return parent.grow_expr(expr, syn=syn)


@sig_to_op.register
def AggrSig_to_op(sig: AggrSig, syn: Apply, parent: Op):
    assert (
        parent.card >= Cardinality.SEQ
        or parent.card == Cardinality.ONE
        and isinstance(parent.sig, JsonAggSig)
    )
    return parent.aggregate(sig)


#
# Embed literal values into query
#


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
def JsonLiteral_embed(_: sa_pg.JSONB):
    return lambda v: sa.cast(sa.literal(json.dumps(v)), sa_pg.JSONB)
