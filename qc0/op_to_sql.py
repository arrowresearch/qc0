from typing import Dict
from functools import singledispatch
import sqlalchemy as sa
from sqlalchemy.sql.selectable import Selectable, Join
from .base import Struct
from .op import (
    Rel,
    RelVoid,
    RelTable,
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
    ExprIdentity,
    ExprConst,
    ExprBinOp,
    ExprTransform,
)


def op_to_sql(op):
    """ Compile operations into SQL."""
    if isinstance(op, Rel):
        return realize_select(rel_to_sql(op, From.empty()))
    elif isinstance(op, Expr):
        return realize_select(expr_to_sql(op, From.empty()))
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
            condition = sa.true()
        current = sa.join(self.current, right, condition)
        joins = self.joins
        return self.replace(current=current, right=right, joins=joins)

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
        current = sa.join(self.current, right, condition)
        joins = {**self.joins, key: right}
        return self.replace(current=current, right=right, joins=joins)

    def join_lateral(self, right):
        condition = sa.true()
        right = right.lateral()
        current = sa.outerjoin(self.current, right, condition)
        joins = self.joins
        return self.replace(current=current, right=right, joins=joins)

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
        return sa.select([value.label("value")])
    elif value is None:
        return from_obj.current.select()
    else:
        return sa.select(
            [value.label("value")], from_obj=from_obj.current
        ).alias()


@singledispatch
def rel_to_sql(rel: Rel, from_obj):
    raise NotImplementedError(  # pragma: no cover
        f"rel_to_sql({type(rel).__name__})"
    )


@rel_to_sql.register
def RelVoid_to_sql(rel: RelVoid, from_obj):
    return None, from_obj


@rel_to_sql.register
def RelTable_to_sql(rel: RelTable, from_obj):
    return None, From.make(rel.table)


@rel_to_sql.register
def RelJoin_to_sql(rel: RelJoin, from_obj):
    value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj)
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
def RelRevJoin_to_sql(rel: RelRevJoin, from_obj):
    if isinstance(rel.rel, RelParent):
        table = rel.fk.parent.table.alias()
        sel = (
            table.select()
            .correlate(from_obj.parent)
            .where(
                table.columns[rel.fk.parent.name]
                == from_obj.parent.columns[rel.fk.column.name]
            )
        )
        return None, From.make(sel)
    else:
        value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj)
        table = rel.fk.parent.table
        condition = lambda left, right: (
            left.columns[rel.fk.column.name]
            == right.columns[rel.fk.parent.name]
        )
        return value, from_obj.join(table, condition)


@rel_to_sql.register
def RelTake_to_sql(rel: RelTake, from_obj):
    val, from_obj = rel_to_sql(rel.rel, from_obj)
    sel = (
        sa.select([from_obj.right], from_obj=from_obj.current)
        .limit(rel.take)
        .alias()
    )
    from_obj = From.make(sel)
    return val, from_obj


@rel_to_sql.register
def RelFilter_to_sql(rel: RelFilter, from_obj):
    val, from_obj = rel_to_sql(rel.rel, from_obj)
    # reparent
    prev_parent, prev_right = from_obj.parent, from_obj.right
    from_obj = from_obj.replace(parent=from_obj.right)
    expr, from_obj = expr_to_sql(rel.expr, from_obj)
    from_obj = from_obj.replace(right=prev_right, parent=prev_parent)
    sel = (
        sa.select([from_obj.right], from_obj=from_obj.current)
        .where(expr)
        .alias()
    )
    from_obj = From.make(sel)
    return val, from_obj


@rel_to_sql.register
def RelParent_to_sql(rel: RelParent, from_obj):
    return None, from_obj


@rel_to_sql.register
def RelExpr_to_sql(rel: RelExpr, from_obj):
    value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj)
    assert value is None
    # reparent
    prev_right, prev_parent = from_obj.right, from_obj.parent
    from_obj = from_obj.replace(parent=from_obj.right)
    expr, from_obj = expr_to_sql(rel.expr, from_obj=from_obj)
    from_obj = from_obj.replace(right=prev_right, parent=prev_parent)
    return expr, from_obj


@singledispatch
def expr_to_sql(expr: Expr, from_obj):
    raise NotImplementedError(  # pragma: no cover
        f"expr_to_sql({type(expr).__name__})"
    )


@expr_to_sql.register
def ExprRel_to_sql(op: ExprRel, from_obj):
    value, from_obj = rel_to_sql(op.rel, from_obj=from_obj)
    return value, from_obj


@expr_to_sql.register
def ExprAggregateRel_to_sql(op: ExprAggregateRel, from_obj):
    value, inner_from_obj = rel_to_sql(op.rel, from_obj=from_obj)
    if op.func is None:
        value = sa.func.jsonb_agg(value).label("value")
    else:
        value = getattr(sa.func, op.func)(value).label("value")
    sel = realize_select((value, inner_from_obj))
    if from_obj.parent is not None:
        from_obj = from_obj.join_lateral(sel)
    else:
        from_obj = from_obj.join(sel)
    return from_obj.right.c.value, from_obj


@expr_to_sql.register
def ExprRecord_to_sql(op: ExprRecord, from_obj):
    args = []
    parent = from_obj.parent
    for field in op.fields.values():
        args.append(sa.literal(field.name))
        expr, from_obj = expr_to_sql(
            field.expr, from_obj=from_obj.replace(parent=parent)
        )
        args.append(expr)
    return sa.func.jsonb_build_object(*args), from_obj


@expr_to_sql.register
def ExprColumn_to_sql(op: ExprColumn, from_obj):
    assert from_obj is not None
    return sa.column(op.column.name, _selectable=from_obj.parent), from_obj


@expr_to_sql.register
def ExprIdentity_to_sql(op: ExprIdentity, from_obj):
    assert from_obj is not None
    pk = []
    for col in op.table.primary_key.columns:
        pk.append(from_obj.parent.columns[col.name])
    return sa.cast(sa.func.row(*pk), sa.String()), from_obj


@expr_to_sql.register
def ExprConst_to_sql(op: ExprConst, from_obj):
    _, from_obj = rel_to_sql(op.rel, from_obj)
    return op.embed(op.value), from_obj


@expr_to_sql.register
def ExprTransform_to_sql(op: ExprTransform, from_obj):
    expr, from_obj = expr_to_sql(op.expr, from_obj)
    return op.transform(expr), from_obj


@expr_to_sql.register
def ExprBinOp_to_sql(op: ExprBinOp, from_obj):
    a, from_obj = expr_to_sql(op.a, from_obj)
    b, from_obj = expr_to_sql(op.b, from_obj)
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
