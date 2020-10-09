from __future__ import annotations
from functools import singledispatch
from enum import IntEnum
from typing import Dict
from sqlalchemy import MetaData, Table
from .base import Struct
from .syn import Syn, Nav, Select
from .op import (
    Op,
    Pipe,
    PipeTable,
    PipeColumn,
    PipeJoin,
    PipeRevJoin,
    PipeParent,
    PipeExpr,
    ExprPipe,
    ExprAggregatePipe,
    ExprRecord,
    ExprColumn,
    Field,
)


def bind(syn: Syn, meta: MetaData):
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
        if self >= o:
            return self
        else:
            return o


class Scope(Struct):
    pass


class UnivScope(Scope):
    tables: Dict[str, Table]


class TableScope(Scope):
    tables: Dict[str, Table]
    table: Table


class EmptyScope(Scope):
    pass


@singledispatch
def to_op(syn: Syn, ctx: Context, parent):
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
            ctx = ctx.replace(
                scope=EmptyScope(), card=ctx.card * Cardinality.ONE
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
        raise NotImplementedError()


@to_op.register
def Select_to_op(syn: Select, ctx: Context, parent: Op):
    parent, ctx = (
        to_op(syn.parent, ctx=ctx, parent=parent)
        if syn.parent
        else (None, ctx)
    )

    fields = {}
    for field in syn.fields.values():
        expr, ectx = to_op(
            field.syn,
            ctx=ctx.replace(card=Cardinality.ONE),
            parent=PipeParent(),
        )
        if isinstance(expr, Pipe):
            if ectx.card == Cardinality.SEQ:
                expr = ExprAggregatePipe(pipe=expr)
            else:
                expr = ExprPipe(pipe=expr)
        fields[field.name] = Field(expr=expr, name=field.name)

    if parent:
        pipe = PipeExpr(pipe=parent, expr=ExprRecord(fields=fields))
        return pipe, ctx
    else:
        expr = ExprRecord(fields=fields)
        return expr, ctx
