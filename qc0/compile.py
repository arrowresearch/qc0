from typing import Dict
from functools import singledispatch
from sqlalchemy import (
    func,
    literal,
    column,
    join,
    outerjoin,
    select,
    true,
)
from sqlalchemy.sql.selectable import Selectable, Join
from .base import Struct
from .op import (
    Rel,
    RelVoid,
    RelTable,
    RelColumn,
    RelJoin,
    RelRevJoin,
    RelParent,
    RelExpr,
    RelTake,
    RelFilter,
    Expr,
    ExprRel,
    ExprAggregateRel,
    ExprRecord,
    ExprColumn,
    ExprConst,
    ExprBinOp,
    ExprTransform,
)


def compile(op):
    """ Compile operations into SQL."""
    if isinstance(op, Rel):
        return realize_select(rel_to_sql(op, From.empty(), None))
    elif isinstance(op, Expr):
        return realize_select(expr_to_sql(op, From.empty(), None))
    else:
        assert False  # pragma: no cover


class From(Struct):
    joins: Dict[any, Selectable]
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
        joins = self.joins
        return From(
            parent=self.parent, current=current, right=right, joins=joins
        )

    def join_parent(self, from_obj, join_on):
        # NOTE(andreypopp): this is a hacky way to dedup joins, need to consider
        # another approach based on structural query equality...
        key = (self.parent.element, from_obj, join_on)
        if key in self.joins:
            return self.replace(right=self.joins[key])
        right = from_obj.alias()
        condition = (
            self.parent.columns[join_on[0]] == right.columns[join_on[1]]
        )
        current = join(self.current, right, condition)
        joins = {**self.joins, key: right}
        return From(
            parent=self.parent, current=current, right=right, joins=joins
        )

    def join_lateral(self, right):
        condition = true()
        right = right.lateral()
        current = outerjoin(self.current, right, condition)
        joins = self.joins
        return From(
            parent=self.parent, current=current, right=right, joins=joins
        )

    @classmethod
    def empty(cls):
        return cls(parent=None, current=None, right=None, joins={})

    @classmethod
    def make(cls, from_obj):
        assert not isinstance(from_obj, Join)
        current = from_obj
        current = current.alias()
        return From(parent=current, current=current, right=current, joins={})


def realize_select(rel):
    value, from_obj = rel
    if from_obj is None:
        return select([value.label("value")])
    elif value is None:
        return from_obj.current.select()
    else:
        return select(
            [value.label("value")], from_obj=from_obj.current
        ).alias()


@singledispatch
def rel_to_sql(rel: Rel, from_obj, parent):
    raise NotImplementedError(  # pragma: no cover
        f"rel_to_sql({type(rel).__name__})"
    )


@rel_to_sql.register
def RelVoid_to_sql(rel: RelVoid, from_obj, parent):
    return None, from_obj


@rel_to_sql.register
def RelTable_to_sql(rel: RelTable, from_obj, parent):
    return None, From.make(rel.table)


@rel_to_sql.register
def RelColumn_to_sql(rel: RelColumn, from_obj, parent):
    value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj, parent=parent)
    assert value is None
    assert from_obj is not None
    return from_obj.right.columns[rel.column.name], from_obj


@rel_to_sql.register
def RelJoin_to_sql(rel: RelJoin, from_obj, parent):
    value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj, parent=parent)
    table = rel.fk.column.table
    condition = lambda left, right: (
        left.columns[rel.fk.parent.name] == right.columns[rel.fk.column.name]
    )
    if isinstance(rel.rel, RelParent):
        from_obj = from_obj.join_parent(
            table, (rel.fk.parent.name, rel.fk.column.name)
        )
    else:
        from_obj = from_obj.join(table, condition)
    return value, from_obj


@rel_to_sql.register
def RelRevJoin_to_sql(rel: RelRevJoin, from_obj, parent):
    if isinstance(rel.rel, RelParent):
        assert parent is not None
        table = rel.fk.parent.table.alias()
        sel = (
            table.select()
            .correlate(parent)
            .where(
                table.columns[rel.fk.parent.name]
                == parent.columns[rel.fk.column.name]
            )
        )
        return None, From.make(sel)
    else:
        value, from_obj = rel_to_sql(
            rel.rel, from_obj=from_obj, parent=parent
        )
        table = rel.fk.parent.table
        condition = lambda left, right: (
            left.columns[rel.fk.column.name]
            == right.columns[rel.fk.parent.name]
        )
        return value, from_obj.join(table, condition)


@rel_to_sql.register
def RelTake_to_sql(rel: RelTake, from_obj, parent):
    val, from_obj = rel_to_sql(rel.rel, from_obj, parent)
    sel = (
        select([from_obj.right], from_obj=from_obj.current)
        .limit(rel.take)
        .alias()
    )
    from_obj = From.make(sel)
    return val, from_obj


@rel_to_sql.register
def RelFilter_to_sql(rel: RelFilter, from_obj, parent):
    val, from_obj = rel_to_sql(rel.rel, from_obj, parent)
    # reparent
    prev_right = from_obj.right
    from_obj = from_obj.replace(parent=from_obj.right)
    expr, from_obj = expr_to_sql(rel.expr, from_obj, parent=from_obj.parent)
    from_obj = from_obj.replace(right=prev_right)
    sel = (
        select([from_obj.right], from_obj=from_obj.current).where(expr).alias()
    )
    from_obj = From.make(sel)
    return val, from_obj


@rel_to_sql.register
def RelParent_to_sql(rel: RelParent, from_obj, parent):
    return None, from_obj


@rel_to_sql.register
def RelExpr_to_sql(rel: RelExpr, from_obj, parent):
    value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj, parent=parent)
    assert value is None
    # reparent
    prev_right = from_obj.right
    from_obj = from_obj.replace(parent=from_obj.right)
    expr, from_obj = expr_to_sql(
        rel.expr, from_obj=from_obj, parent=from_obj.parent
    )
    from_obj = from_obj.replace(right=prev_right)
    return expr, from_obj


@singledispatch
def expr_to_sql(expr: Expr, from_obj, parent):
    raise NotImplementedError(  # pragma: no cover
        f"expr_to_sql({type(expr).__name__})"
    )


@expr_to_sql.register
def ExprRel_to_sql(op: ExprRel, from_obj, parent):
    value, from_obj = rel_to_sql(op.rel, from_obj=from_obj, parent=parent)
    return value, from_obj


@expr_to_sql.register
def ExprAggregateRel_to_sql(op: ExprAggregateRel, from_obj, parent):
    value, inner_from_obj = rel_to_sql(op.rel, from_obj=None, parent=parent)
    if op.func is None:
        value = func.jsonb_agg(value).label("value")
    else:
        value = getattr(func, op.func)(value).label("value")
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


@expr_to_sql.register
def ExprConst_to_sql(op: ExprConst, from_obj, parent):
    _, from_obj = rel_to_sql(op.rel, from_obj, parent)
    return op.embed(op.value), from_obj


@expr_to_sql.register
def ExprTransform_to_sql(op: ExprTransform, from_obj, parent):
    expr, from_obj = expr_to_sql(op.expr, from_obj, parent)
    return op.transform(expr), from_obj


@expr_to_sql.register
def ExprBinOp_to_sql(op: ExprBinOp, from_obj, parent):
    a, from_obj = expr_to_sql(op.a, from_obj, parent)
    b, from_obj = expr_to_sql(op.b, from_obj, parent)
    if op.func == "__eq__":
        return a == b, from_obj
    if op.func == "__ne__":
        return a != b, from_obj
    if op.func == "__add__":
        return a + b, from_obj
    if op.func == "__sub__":
        return a - b, from_obj
    if op.func == "__mul__":
        return a * b, from_obj
    if op.func == "__truediv__":
        return a / b, from_obj
    if op.func == "__and__":
        return a & b, from_obj
    if op.func == "__or__":
        return a | b, from_obj
    else:
        assert False, f"unknown operation {op.func}"  # pragma: no cover
