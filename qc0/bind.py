from __future__ import annotations
import json
from functools import singledispatch
from enum import IntEnum
from typing import Dict, Tuple, Callable, Any, Union, Optional
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
    """ Bind syntax to database catalogue and produce a pipeline of operations."""
    ctx = to_op(syn, ctx=BindingContext.initial(meta))
    assert ctx.op is not None
    return ctx.op


class BindingContext(Struct):
    scope: Scope
    card: Cardinality
    op: Optional[Op]

    @classmethod
    def initial(cls, meta: sa.MetaData) -> BindingContext:
        return cls(
            scope=UnivScope(tables=meta.tables),
            card=Cardinality.ONE,
            op=None,
        )


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
def to_op(syn: Syn, ctx: BindingContext):
    """ Produce an operation out of a query."""
    raise NotImplementedError(type(syn))


@to_op.register
def Nav_to_op(syn: Nav, ctx: BindingContext):
    if syn.parent is not None:
        ctx = to_op(syn.parent, ctx=ctx)

    if isinstance(ctx.scope, UnivScope):
        table = ctx.scope.tables[syn.name]
        return ctx.replace(
            scope=TableScope(table=table, tables=ctx.scope.tables),
            card=Cardinality.SEQ,
            op=PipeTable(table=table),
        )

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
            if isinstance(ctx.op, PipeParent):
                op = ExprColumn(column=column)
            else:
                op = PipeColumn(pipe=ctx.op, column=column)
            return ctx.replace(
                scope=next_scope,
                card=ctx.card * Cardinality.ONE,
                op=op,
            )

        elif syn.name in fks:
            fk = fks[syn.name]
            return ctx.replace(
                scope=TableScope(
                    table=fk.column.table, tables=ctx.scope.tables
                ),
                card=ctx.card * Cardinality.ONE,
                op=PipeJoin(pipe=ctx.op, fk=fk),
            )

        elif syn.name in rev_fks:
            fk = rev_fks[syn.name]
            return ctx.replace(
                scope=TableScope(
                    table=fk.parent.table, tables=ctx.scope.tables
                ),
                card=ctx.card * Cardinality.SEQ,
                op=PipeRevJoin(pipe=ctx.op, fk=fk),
            )
        else:
            assert False, f"Unable to lookup {syn.name}"

    elif isinstance(ctx.scope, RecordScope):
        if syn.name in ctx.scope.fields:
            field_syn, field_ctx = ctx.scope.fields[syn.name]
            return to_op(field_syn, ctx=field_ctx)
        else:
            assert False, f"Unable to lookup {syn.name}"

    elif isinstance(ctx.scope, SyntheticScope):
        next_scope, transform = ctx.scope.lookup(syn.name)
        assert transform is not None, f"Unable to lookup {syn.name}"
        op = ctx.op
        if isinstance(op, Pipe):
            op = ExprPipe(pipe=op)
        op = ExprTransform(expr=op, transform=transform)
        return ctx.replace(scope=next_scope, op=op)
    else:
        raise NotImplementedError()


@to_op.register
def Select_to_op(syn: Select, ctx: BindingContext):
    if syn.parent:
        ctx = to_op(syn.parent, ctx=ctx)

    fields = {}
    scope_fields = {}
    for field in syn.fields.values():
        fctx = to_op(
            field.syn,
            ctx=ctx.replace(card=Cardinality.ONE, op=PipeParent()),
        )
        if isinstance(fctx.op, Pipe):
            if fctx.card == Cardinality.SEQ:
                op = ExprAggregatePipe(pipe=fctx.op, func=None)
            else:
                op = ExprPipe(pipe=fctx.op)
            fctx = fctx.replace(op=op)
        fields[field.name] = Field(expr=fctx.op, name=field.name)
        scope_fields[field.name] = field.syn, ctx

    next_scope = RecordScope(fields=scope_fields)
    if ctx.op:
        return ctx.replace(
            scope=next_scope,
            op=PipeExpr(pipe=ctx.op, expr=ExprRecord(fields=fields)),
        )
    else:
        return ctx.replace(op=ExprRecord(fields=fields), scope=next_scope)


@to_op.register
def Apply_to_op(syn: Apply, ctx: BindingContext):
    if syn.name in {"count", "exists", "sum"}:
        assert (
            len(syn.args) == 1
        ), f"{syn.name}(...): expected a single argument"
        arg = syn.args[0]
        ctx = to_op(arg, ctx)
        assert isinstance(ctx.op, Pipe), f"{syn.name}(...): requires a pipe"
        assert (
            ctx.card >= Cardinality.SEQ
        ), "{syn.name}(...): expected a sequence of items"
        return ctx.replace(op=ExprAggregatePipe(pipe=ctx.op, func=syn.name))
    elif syn.name == "take":
        assert len(syn.args) == 2, "take(...): expected exactly two arguments"
        arg, take = syn.args
        ctx = to_op(arg, ctx)
        assert isinstance(ctx.op, Pipe), "take(...): requires a pipe"
        return ctx.replace(op=PipeTake(pipe=ctx.op, take=take))
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
        actx = to_op(a, ctx)
        if isinstance(actx.op, Pipe):
            actx = actx.replace(op=ExprPipe(actx.op))
        bctx = to_op(b, ctx)
        if isinstance(bctx.op, Pipe):
            bctx = bctx.replace(op=ExprPipe(bctx.op))
        return ctx.replace(op=ExprBinOp(func=syn.name, a=actx.op, b=bctx.op))
    elif syn.name == "filter":
        assert (
            len(syn.args) == 2
        ), "filter(...): expected exactly two arguments"
        arg, expr = syn.args
        ctx = to_op(arg, ctx)
        assert isinstance(ctx.op, Pipe), "filter(...): requires a pipe"
        ectx = to_op(expr, ctx=ctx.replace(op=PipeParent()))
        if isinstance(ectx.op, Pipe):
            ectx = ectx.replace(op=ExprPipe(ectx.op))
        return ctx.replace(op=PipeFilter(pipe=ctx.op, expr=ectx.op))
    else:
        assert False, f"Unknown {syn.name}(...) combinator"


@to_op.register
def Literal_to_op(syn: Literal, ctx: BindingContext):
    return ctx.replace(
        scope=type_scope(syn.type),
        op=ExprConst(value=syn.value, embed=embed(syn.type)),
    )


@to_op.register
def Compose_to_op(syn: Compose, ctx: BindingContext):
    ctx = to_op(syn.a, ctx)
    ctx = to_op(syn.b, ctx)
    return ctx


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
