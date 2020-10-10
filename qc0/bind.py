from __future__ import annotations
import json
from functools import singledispatch
from enum import IntEnum
from typing import Dict, Tuple, Callable, Any, Union
import sqlalchemy as sa
from .base import Struct
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
    PipeTable,
    PipeColumn,
    PipeJoin,
    PipeRevJoin,
    PipeParent,
    PipeExpr,
    PipeTake,
    PipeFilter,
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
    """ Bind syntax to metadata and produce a pipeline of operations."""
    ctx = Context(scope=UnivScope(tables=meta.tables), card=Cardinality.ONE)
    op, _ctx = to_op(syn, ctx=ctx, parent=None)
    return op


class Context(Struct):
    scope: Scope
    card: Cardinality


class Cardinality(IntEnum):
    ONE = 1
    SEQ = 2

    def __mul__(self, o: Cardinality):
        assert isinstance(o, Cardinality)
        return self if self >= o else o


class Scope(Struct):
    pass


class UnivScope(Scope):
    tables: Dict[str, sa.Table]


class TableScope(Scope):
    tables: Dict[str, sa.Table]
    table: sa.Table


class RecordScope(Scope):
    fields: Dict[str, Syn]


class SyntheticScope(Scope):
    def lookup(self, name) -> Tuple[Scope, Callable[Any, Any]]:
        raise NotImplementedError()


class JsonScope(SyntheticScope):
    def lookup(self, name):
        return self, lambda v: v[name]


class DateScope(SyntheticScope):
    def lookup(self, name):
        names = {
            "year": lambda v: sa.extract("year", v),
            "month": lambda v: sa.extract("month", v),
            "day": lambda v: sa.extract("day", v),
        }
        return EmptyScope(), names[name]


class EmptyScope(Scope):
    pass


@singledispatch
def to_op(syn: Syn, ctx: Context, parent):
    """ Produce an operation out of a query."""
    raise NotImplementedError(type(syn))


@to_op.register
def Nav_to_op(syn: Nav, ctx: Context, parent: Op):
    if syn.parent is not None:
        parent, ctx = to_op(syn.parent, ctx=ctx, parent=parent)

    if isinstance(ctx.scope, UnivScope):
        table = ctx.scope.tables[syn.name]
        ctx = ctx.replace(
            scope=TableScope(table=table, tables=ctx.scope.tables),
            card=Cardinality.SEQ,
        )
        return PipeTable(table=table), ctx

    elif isinstance(ctx.scope, TableScope):
        tables = ctx.scope.tables
        table = ctx.scope.table
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
            ctx = ctx.replace(
                scope=next_scope, card=ctx.card * Cardinality.ONE
            )
            if isinstance(parent, PipeParent):
                return ExprColumn(column=column), ctx
            else:
                return PipeColumn(pipe=parent, column=column), ctx

        elif syn.name in fks:
            fk = fks[syn.name]
            ctx = ctx.replace(
                scope=TableScope(
                    table=fk.column.table, tables=ctx.scope.tables
                ),
                card=ctx.card * Cardinality.ONE,
            )
            return PipeJoin(pipe=parent, fk=fk), ctx

        elif syn.name in rev_fks:
            fk = rev_fks[syn.name]
            ctx = ctx.replace(
                scope=TableScope(
                    table=fk.parent.table, tables=ctx.scope.tables
                ),
                card=ctx.card * Cardinality.SEQ,
            )
            return PipeRevJoin(pipe=parent, fk=fk), ctx
        else:
            assert False, f"Unable to lookup {syn.name}"

    elif isinstance(ctx.scope, RecordScope):
        if syn.name in ctx.scope.fields:
            field_syn, field_ctx, field_parent = ctx.scope.fields[syn.name]
            return to_op(field_syn, ctx=field_ctx, parent=field_parent)
        else:
            assert False, f"Unable to lookup {syn.name}"

    elif isinstance(ctx.scope, SyntheticScope):
        next_scope, transform = ctx.scope.lookup(syn.name)
        assert transform is not None, f"Unable to lookup {syn.name}"
        if isinstance(parent, Pipe):
            parent = ExprPipe(pipe=parent)
        op = ExprTransform(expr=parent, transform=transform)
        return op, ctx.replace(scope=next_scope)
    else:
        raise NotImplementedError()


@to_op.register
def Select_to_op(syn: Select, ctx: Context, parent: Op):
    parent, ctx = (
        to_op(syn.parent, ctx=ctx, parent=parent)
        if syn.parent
        else (parent, ctx)
    )

    fields = {}
    scope_fields = {}
    for field in syn.fields.values():
        expr, ectx = to_op(
            field.syn,
            ctx=ctx.replace(card=Cardinality.ONE),
            parent=PipeParent(),
        )
        if isinstance(expr, Pipe):
            if ectx.card == Cardinality.SEQ:
                expr = ExprAggregatePipe(pipe=expr, func=None)
            else:
                expr = ExprPipe(pipe=expr)
        fields[field.name] = Field(expr=expr, name=field.name)
        scope_fields[field.name] = field.syn, ctx, parent

    ctx = ctx.replace(scope=RecordScope(fields=scope_fields))

    if parent:
        pipe = PipeExpr(pipe=parent, expr=ExprRecord(fields=fields))
        return pipe, ctx
    else:
        expr = ExprRecord(fields=fields)
        return expr, ctx


@to_op.register
def Apply_to_op(syn: Apply, ctx: Context, parent: Op):
    if syn.name in {"count", "exists", "sum"}:
        assert (
            len(syn.args) == 1
        ), f"{syn.name}(...): expected a single argument"
        arg = syn.args[0]
        op, ctx = to_op(arg, ctx, parent)
        assert isinstance(op, Pipe), f"{syn.name}(...): requires a pipe"
        assert (
            ctx.card >= Cardinality.SEQ
        ), "{syn.name}(...): expected a sequence of items"
        op = ExprAggregatePipe(pipe=op, func=syn.name)
        return op, ctx
    elif syn.name == "take":
        assert len(syn.args) == 2, "take(...): expected exactly two arguments"
        arg, take = syn.args
        op, ctx = to_op(arg, ctx, parent)
        assert isinstance(op, Pipe), "take(...): requires a pipe"
        op = PipeTake(pipe=op, take=take)
        return op, ctx
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
        a, actx = to_op(a, ctx, parent)
        if isinstance(a, Pipe):
            a = ExprPipe(a)
        b, bctx = to_op(b, ctx, parent)
        if isinstance(b, Pipe):
            b = ExprPipe(b)
        op = ExprBinOp(func=syn.name, a=a, b=b)
        return op, ctx
    elif syn.name == "filter":
        assert (
            len(syn.args) == 2
        ), "filter(...): expected exactly two arguments"
        arg, expr = syn.args
        op, ctx = to_op(arg, ctx, parent)
        assert isinstance(op, Pipe), "filter(...): requires a pipe"
        expr, _ctx = to_op(expr, ctx, parent=PipeParent())
        if isinstance(expr, Pipe):
            expr = ExprPipe(expr)
        op = PipeFilter(pipe=op, expr=expr)
        return op, ctx
    else:
        assert False, f"Unknown {syn.name}(...) combinator"


@to_op.register
def Literal_to_op(syn: Literal, ctx: Context, parent: Op):
    next_scope = type_scope(syn.type)
    ctx = ctx.replace(scope=next_scope)
    return ExprConst(value=syn.value, embed=embed(syn.type)), ctx


@to_op.register
def Compose_to_op(syn: Compose, ctx: Context, parent: Op):
    op, ctx = to_op(syn.a, ctx, parent)
    op, ctx = to_op(syn.b, ctx, parent=op)
    return op, ctx


@singledispatch
def embed(v: sa.Type):
    """ Describe how to make a query out of a value."""
    raise NotImplementedError(
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


@singledispatch
def type_scope(_: sa.Type) -> Union[Scope, SyntheticScope]:
    """ Describe scope for a specified type. """
    return EmptyScope()


@type_scope.register
def Date_scalar_scope(_: sa.Date):
    return DateScope()


@type_scope.register
def Json_scalar_scope(_: sa.dialects.postgresql.JSONB):
    return JsonScope()
