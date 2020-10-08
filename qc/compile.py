from functools import singledispatch
from typing import Dict
from sqlalchemy import (
    func,
    literal,
    column,
    join,
    select,
)
from sqlalchemy.sql.selectable import Selectable, Join
from .base import Struct
from .op import (
    Pipe,
    PipeTable,
    PipeColumn,
    PipeJoin,
    PipeParent,
    PipeExpr,
    Expr,
    ExprPipe,
    ExprRecord,
    ExprColumn,
)


def compile(op):
    """ Compile operations pipeline into SQL."""
    if isinstance(op, Pipe):
        return realize_select(pipe_to_sql(op))
    elif isinstance(op, Expr):
        return realize_select(expr_to_sql(op))
    else:
        assert False


class From(Struct):
    items: Dict[Selectable, Selectable]
    parent: Selectable
    current: Selectable
    right: Selectable

    def join(self, from_obj, condition):
        if from_obj in self.items:
            return From(
                parent=self.parent,
                current=self.current,
                right=self.items[from_obj],
                items=self.items,
            )
        else:
            right = from_obj.alias(from_obj.name)
            current = join(self.current, right)
            items = {**self.items, from_obj: right}
            return From(
                parent=self.parent, current=current, right=right, items=items
            )

    @classmethod
    def make(cls, from_obj):
        assert not isinstance(from_obj, Join)
        current = from_obj
        current = current.alias(current.name)
        items = {from_obj: current}
        return From(
            parent=current, current=current, right=current, items=items
        )


def realize_select(pipe):
    value, from_obj = pipe
    assert isinstance(from_obj, From)
    if value is None:
        return from_obj.current.select()
    else:
        return select([value.label("value")], from_obj=from_obj.current)


@singledispatch
def pipe_to_sql(pipe: Pipe, from_obj=None):
    raise NotImplementedError(type(pipe))


@pipe_to_sql.register
def PipeTable_to_sql(pipe: PipeTable, from_obj=None):
    return None, From.make(pipe.table)


@pipe_to_sql.register
def PipeColumn_to_sql(pipe: PipeColumn, from_obj=None):
    value, from_obj = pipe_to_sql(pipe.pipe, from_obj=from_obj)
    assert value is None
    assert from_obj is not None
    value = column(pipe.column.name, _selectable=from_obj.right)
    return value, from_obj


@pipe_to_sql.register
def PipeJoin_to_sql(pipe: PipeJoin, from_obj):
    value, from_obj = pipe_to_sql(pipe.pipe, from_obj=from_obj)
    table = pipe.fk.column.table
    return value, from_obj.join(table, None)


@pipe_to_sql.register
def PipeParent_to_sql(pipe: PipeParent, from_obj):
    return None, from_obj


@pipe_to_sql.register
def PipeExpr_to_sql(pipe: PipeExpr, from_obj=None):
    value, from_obj = pipe_to_sql(pipe.pipe, from_obj=from_obj)
    assert value is None
    # reparent
    prev_parent = from_obj.parent
    from_obj = from_obj.replace(parent=from_obj.right)
    expr, from_obj = expr_to_sql(pipe.expr, from_obj=from_obj)
    from_obj = from_obj.replace(parent=prev_parent)
    return expr, from_obj


@singledispatch
def expr_to_sql(expr: Expr, from_obj=None):
    raise NotImplementedError(type(expr))


@expr_to_sql.register
def ExprPipe_to_sql(op: ExprPipe, from_obj=None):
    value, from_obj = pipe_to_sql(op.pipe, from_obj=from_obj)
    return value, from_obj


@expr_to_sql.register
def ExprRecord_to_sql(op: ExprRecord, from_obj=None):
    args = []
    for field in op.fields.values():
        args.append(literal(field.name))
        expr, from_obj = expr_to_sql(field.expr, from_obj=from_obj)
        args.append(expr)
    return func.json_build_object(*args), from_obj


@expr_to_sql.register
def ExprColumn_to_sql(op: ExprColumn, from_obj=None):
    assert from_obj is not None
    return column(op.column.name, _selectable=from_obj.parent), from_obj
