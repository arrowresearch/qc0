from __future__ import annotations
from functools import singledispatch
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
    PipeParent,
    PipeExpr,
    ExprPipe,
    ExprRecord,
    ExprColumn,
    Field,
)


def bind(syn: Syn, meta: MetaData):
    """ Bind syntax to metadata and produce a pipeline of operations."""
    ctx = Context(scope=UnivScope(tables=meta.tables))
    op, _ctx = to_op(syn, ctx=ctx, parent=None)
    return op


class Context(Struct):
    scope: Scope


class Scope(Struct):
    pass


class UnivScope(Scope):
    tables: Dict[str, Table]


class TableScope(Scope):
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
        ctx = ctx.replace(scope=TableScope(table=table))
        return PipeTable(table=table), ctx

    elif isinstance(ctx.scope, TableScope):
        table = ctx.scope.table
        fks = {f.column.table.name: f for f in table.foreign_keys}

        if syn.name in table.columns:
            column = table.columns[syn.name]
            ctx = ctx.replace(scope=EmptyScope())
            if isinstance(parent, PipeParent):
                return ExprColumn(column=column), ctx
            else:
                return PipeColumn(pipe=parent, column=column), ctx

        elif syn.name in fks:
            fk = fks[syn.name]
            ctx = ctx.replace(scope=TableScope(table=fk.column.table))
            return PipeJoin(pipe=parent, fk=fk), ctx

        else:
            # TODO(andreypopp): implement lookup for reverse fks
            raise NotImplementedError()

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
        expr, _ = to_op(field.syn, ctx=ctx, parent=PipeParent())
        if isinstance(expr, Pipe):
            # TODO(andreypopp): here we need to check for cardinality and wrap
            # into ExprAggregatePipe if it's a seq one.
            expr = ExprPipe(pipe=expr)
        fields[field.name] = Field(expr=expr, name=field.name)

    if parent:
        pipe = PipeExpr(pipe=parent, expr=ExprRecord(fields=fields))
        return pipe, ctx
    else:
        expr = ExprRecord(fields=fields)
        return expr, ctx
