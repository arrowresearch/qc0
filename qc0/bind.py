"""

    qc0.bind
    ========

"""

from __future__ import annotations
import json
from functools import singledispatch
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
    Pipe,
    PipeVoid,
    PipeTable,
    PipeColumn,
    PipeJoin,
    PipeRevJoin,
    PipeParent,
    PipeExpr,
    PipeTake,
    PipeFilter,
    Expr,
    ExprPipe,
    ExprAggregatePipe,
    ExprRecord,
    ExprColumn,
    ExprConst,
    ExprBinOp,
    ExprTransform,
    Field,
)


def bind(syn: Syn, meta: sa.MetaData):
    """ Bind syntax to database catalogue and produce a pipeline of operations."""
    parent = PipeVoid(
        scope=UnivScope(tables=meta.tables),
        card=Cardinality.ONE,
    )
    return build_op(syn, parent)


def build_op(syn: Syn, parent: Op):
    op = to_op(syn, parent=parent)
    op = build_selection(op)
    return op


def build_selection(op: Op):
    if isinstance(op.scope, RecordScope):
        parent = PipeParent(
            scope=op.scope.scope,
            card=Cardinality.ONE,
        )
        fields = {}
        for name, f in op.scope.fields.items():
            field = build_op(
                f.syn,
                parent,
            )
            if isinstance(field, Pipe):
                if field.card == Cardinality.SEQ:
                    field = ExprAggregatePipe.wrap(
                        field,
                        pipe=field,
                        func=None,
                        card=Cardinality.ONE,
                    )
                else:
                    field = ExprPipe.wrap(field, pipe=field)
            fields[name] = Field(expr=field, name=name)

        expr = ExprRecord.wrap(op, fields=fields)
        op = PipeExpr.wrap(op, pipe=op, expr=expr)
    return op


@singledispatch
def to_op(syn: Syn, parent: Op):
    """ Produce an operation out of a query."""
    raise NotImplementedError(type(syn))  # pragma: no cover


@to_op.register
def None_to_op(syn: type(None), parent: Op):
    return parent


@to_op.register
def Nav_to_op(syn: Nav, parent: Op):

    if isinstance(parent.scope, UnivScope):
        table = parent.scope.tables[syn.name]
        return PipeTable(
            table=table,
            scope=TableScope(table=table, tables=parent.scope.tables),
            card=Cardinality.SEQ,
        )

    elif isinstance(parent.scope, TableScope):
        tables = parent.scope.tables
        table = parent.scope.table
        fks = {fk.column.table.name: fk for fk in table.foreign_keys}
        rev_fks = {
            fk.parent.table.name: fk
            for t in tables.values()
            for fk in t.foreign_keys
            if fk.column.table == table
        }

        if syn.name in table.columns:
            column = table.columns[syn.name]
            next_scope = type_scope(column.type)
            if isinstance(parent, PipeParent):
                return ExprColumn(
                    column=column,
                    scope=next_scope,
                    card=parent.card * Cardinality.ONE,
                )
            else:
                return PipeColumn(
                    pipe=parent,
                    column=column,
                    scope=next_scope,
                    card=parent.card * Cardinality.ONE,
                )

        elif syn.name in fks:
            fk = fks[syn.name]
            return PipeJoin(
                pipe=parent,
                fk=fk,
                scope=TableScope(
                    table=fk.column.table, tables=parent.scope.tables
                ),
                card=parent.card * Cardinality.ONE,
            )

        elif syn.name in rev_fks:
            fk = rev_fks[syn.name]
            return PipeRevJoin(
                pipe=parent,
                fk=fk,
                scope=TableScope(
                    table=fk.parent.table, tables=parent.scope.tables
                ),
                card=parent.card * Cardinality.SEQ,
            )
        else:
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
        next_scope, transform = parent.scope.lookup(syn.name)
        assert transform is not None, f"Unable to lookup {syn.name}"
        if isinstance(parent, Pipe):
            parent = ExprPipe.wrap(parent, pipe=parent)
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
    scope = RecordScope(scope=parent.scope, fields=syn.fields)
    return parent.replace(scope=scope)


@to_op.register
def Apply_to_op(syn: Apply, parent: Op):
    if syn.name in {"count", "exists", "sum"}:
        assert len(syn.args) == 0, f"{syn.name}(...): expected no arguments"
        assert isinstance(parent, Pipe), f"{syn.name}(...): requires a pipe"
        assert (
            parent.card >= Cardinality.SEQ
        ), "{syn.name}(...): expected a sequence of items"
        return ExprAggregatePipe.wrap(parent, pipe=parent, func=syn.name)
    elif syn.name == "take":
        assert len(syn.args) == 1, "take(...): expected a single argument"
        take = syn.args[0]
        assert isinstance(parent, Pipe), "take(...): requires a pipe"
        return PipeTake.wrap(parent, pipe=parent, take=take)
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
        if isinstance(a, Pipe):
            a = ExprPipe.wrap(a, pipe=a)
        b = to_op(b, parent)
        if isinstance(b, Pipe):
            b = ExprPipe.wrap(b, pipe=b)
        return ExprBinOp.wrap(
            parent,
            func=syn.name,
            a=a,
            b=b,
        )
    elif syn.name == "filter":
        assert len(syn.args) == 1, "filter(...): expected a single argument"
        expr = syn.args[0]
        assert isinstance(parent, Pipe), "filter(...): requires a pipe"
        expr = to_op(
            expr,
            PipeParent(
                scope=parent.scope,
                card=Cardinality.ONE,
            ),
        )
        assert isinstance(expr, Expr)
        return PipeFilter.wrap(
            parent,
            pipe=parent,
            expr=expr,
            card=parent.card,
        )
    else:
        assert False, f"Unknown {syn.name}(...) combinator"  # pragma: no cover


@to_op.register
def Literal_to_op(syn: Literal, parent: Op):
    # If the parent is another expression, we just ignore it
    if isinstance(parent, Expr):
        parent = PipeVoid(
            scope=EmptyScope(),
            card=Cardinality.ONE,
        )
    return ExprConst(
        pipe=parent,
        value=syn.value,
        embed=embed(syn.type),
        scope=type_scope(syn.type),
        card=parent.card,
    )


@to_op.register
def Compose_to_op(syn: Compose, parent: Op):
    parent = to_op(syn.a, parent)
    parent = to_op(syn.b, parent)
    return parent


@singledispatch
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
