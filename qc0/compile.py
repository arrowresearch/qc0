from functools import singledispatch
from sqlalchemy import (
    func,
    literal,
    column,
    join,
    outerjoin,
    select,
    true,
    text,
)
from sqlalchemy.sql.selectable import Selectable, Join
from .base import Struct
from .op import (
    Pipe,
    PipeTable,
    PipeColumn,
    PipeJoin,
    PipeRevJoin,
    PipeParent,
    PipeExpr,
    Expr,
    ExprPipe,
    ExprAggregatePipe,
    ExprRecord,
    ExprColumn,
)


def compile(op):
    """ Compile operations pipeline into SQL."""
    if isinstance(op, Pipe):
        return realize_select(pipe_to_sql(op, From.empty(), None))
    elif isinstance(op, Expr):
        return realize_select(expr_to_sql(op, From.empty(), None))
    else:
        assert False


class From(Struct):
    parent: Selectable
    current: Selectable
    right: Selectable

    def join(self, from_obj, condition=None):
        if self.current is None:
            assert condition is None
            return self.make(from_obj.alias())
        right = from_obj.alias()
        if condition is not None:
            condition = condition(self.right, right)
        else:
            condition = true()
        current = join(self.current, right, condition)
        return From(parent=self.parent, current=current, right=right)

    def join_lateral(self, right):
        condition = true()
        right = right.lateral()
        current = outerjoin(self.current, right, condition)
        return From(parent=self.parent, current=current, right=right)

    @classmethod
    def empty(cls):
        return cls(parent=None, current=None, right=None)

    @classmethod
    def make(cls, from_obj):
        assert not isinstance(from_obj, Join)
        current = from_obj
        current = current.alias()
        return From(parent=current, current=current, right=current)


def realize_select(pipe):
    value, from_obj = pipe
    if from_obj is None:
        return select([value.label("value")])
    elif value is None:
        return from_obj.current.select()
    else:
        return select(
            [value.label("value")], from_obj=from_obj.current
        ).alias()


@singledispatch
def pipe_to_sql(pipe: Pipe, from_obj, parent):
    raise NotImplementedError(type(pipe))


@pipe_to_sql.register
def PipeTable_to_sql(pipe: PipeTable, from_obj, parent):
    return None, From.make(pipe.table)


@pipe_to_sql.register
def PipeColumn_to_sql(pipe: PipeColumn, from_obj, parent):
    value, from_obj = pipe_to_sql(pipe.pipe, from_obj=from_obj, parent=parent)
    assert value is None
    assert from_obj is not None
    return from_obj.right.columns[pipe.column.name], from_obj


@pipe_to_sql.register
def PipeJoin_to_sql(pipe: PipeJoin, from_obj, parent):
    value, from_obj = pipe_to_sql(pipe.pipe, from_obj=from_obj, parent=parent)
    table = pipe.fk.column.table
    condition = lambda left, right: (
        left.columns[pipe.fk.parent.name] == right.columns[pipe.fk.column.name]
    )
    return value, from_obj.join(table, condition)


@pipe_to_sql.register
def PipeRevJoin_to_sql(pipe: PipeRevJoin, from_obj, parent):
    if isinstance(pipe.pipe, PipeParent):
        assert parent is not None
        table = pipe.fk.parent.table.alias()
        sel = (
            table.select()
            .correlate(parent)
            .where(
                table.columns[pipe.fk.parent.name]
                == parent.columns[pipe.fk.column.name]
            )
        )
        return None, From.make(sel)
    else:
        value, from_obj = pipe_to_sql(
            pipe.pipe, from_obj=from_obj, parent=parent
        )
        table = pipe.fk.parent.table
        condition = lambda left, right: (
            left.columns[pipe.fk.column.name]
            == right.columns[pipe.fk.parent.name]
        )
        return value, from_obj.join(table, condition)


@pipe_to_sql.register
def PipeParent_to_sql(pipe: PipeParent, from_obj, parent):
    return None, from_obj


@pipe_to_sql.register
def PipeExpr_to_sql(pipe: PipeExpr, from_obj, parent):
    value, from_obj = pipe_to_sql(pipe.pipe, from_obj=from_obj, parent=parent)
    assert value is None
    # reparent
    prev_parent = from_obj.parent
    from_obj = from_obj.replace(parent=from_obj.right)
    expr, from_obj = expr_to_sql(
        pipe.expr, from_obj=from_obj, parent=from_obj.parent
    )
    from_obj = from_obj.replace(parent=prev_parent)
    return expr, from_obj


@singledispatch
def expr_to_sql(expr: Expr, from_obj, parent):
    raise NotImplementedError(type(expr))


@expr_to_sql.register
def ExprPipe_to_sql(op: ExprPipe, from_obj, parent):
    value, from_obj = pipe_to_sql(op.pipe, from_obj=from_obj, parent=parent)
    return value, from_obj


@expr_to_sql.register
def ExprAggregatePipe_to_sql(op: ExprAggregatePipe, from_obj, parent):
    value, inner_from_obj = pipe_to_sql(op.pipe, from_obj=None, parent=parent)
    value = func.jsonb_agg(value).label("value")
    sel = realize_select((value, inner_from_obj))
    if parent is not None:
        from_obj = from_obj.join_lateral(sel)
    else:
        from_obj = from_obj.join(sel)
    return from_obj.right.c.value, from_obj


@expr_to_sql.register
def ExprRecord_to_sql(op: ExprRecord, from_obj, parent):
    args = []
    for field in op.fields.values():
        args.append(literal(field.name))
        expr, from_obj = expr_to_sql(
            field.expr, from_obj=from_obj, parent=parent
        )
        args.append(expr)
    return func.jsonb_build_object(*args), from_obj


@expr_to_sql.register
def ExprColumn_to_sql(op: ExprColumn, from_obj, parent):
    assert from_obj is not None
    return column(op.column.name, _selectable=from_obj.parent), from_obj
