from typing import Dict
from functools import singledispatch
import sqlalchemy as sa
from sqlalchemy.sql.selectable import Selectable, Join, Alias
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
    existing: Dict[any, Selectable]
    current: Selectable
    at: Selectable

    def join_at(self, from_obj, join_on=None):
        if self.current is None:
            assert join_on is None
            from_obj = from_obj.alias()

            return self.make(from_obj), from_obj

        # NOTE(andreypopp): this is a hacky way to dedup existing, need to consider
        # another approach based on structural query equality...
        key = (
            self.at.element if self.at is not None else None,
            from_obj,
            join_on,
        )
        if key in self.existing:
            at = self.existing[key]
            return self.replace(at=at), at

        at = from_obj.alias()
        condition = (
            self.at.columns[join_on[0]] == at.columns[join_on[1]]
            if join_on
            else sa.true()
        )
        current = sa.join(self.current, at, condition)
        next = self.replace(
            current=current, at=at, existing={**self.existing, key: at}
        )
        return next, at

    def join_lateral(self, from_obj):
        condition = sa.true()
        at = from_obj.lateral()
        current = sa.outerjoin(self.current, at, condition)
        next = self.replace(current=current, at=at, existing=self.existing)
        return next, at

    @classmethod
    def empty(cls):
        return cls(current=None, at=None, existing={})

    @classmethod
    def make(cls, from_obj):
        assert not isinstance(from_obj, Join)
        if not isinstance(from_obj, Alias):
            from_obj = from_obj.alias()
        return From(current=from_obj, at=from_obj, existing={})


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
    from_obj, _ = from_obj.join_at(
        table, (rel.fk.parent.name, rel.fk.column.name)
    )
    return value, from_obj


@rel_to_sql.register
def RelRevJoin_to_sql(rel: RelRevJoin, from_obj):
    if isinstance(rel.rel, RelParent):
        table = rel.fk.parent.table.alias()
        sel = (
            table.select()
            .correlate(from_obj.at)
            .where(
                table.columns[rel.fk.parent.name]
                == from_obj.at.columns[rel.fk.column.name]
            )
        )
        return None, From.make(sel)
    else:
        value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj)
        table = rel.fk.parent.table
        from_obj, _ = from_obj.join_at(
            table, (rel.fk.column.name, rel.fk.parent.name)
        )
        return value, from_obj


@rel_to_sql.register
def RelTake_to_sql(rel: RelTake, from_obj):
    val, from_obj = rel_to_sql(rel.rel, from_obj)
    sel = (
        sa.select([from_obj.at], from_obj=from_obj.current)
        .limit(rel.take)
        .alias()
    )
    from_obj = From.make(sel)
    return val, from_obj


@rel_to_sql.register
def RelFilter_to_sql(rel: RelFilter, from_obj):
    val, from_obj = rel_to_sql(rel.rel, from_obj)
    # reparent
    prev_at = from_obj.at
    expr, inner_from_obj = expr_to_sql(rel.expr, from_obj)
    from_obj = from_obj.replace(current=inner_from_obj.current)
    sel = sa.select([prev_at], from_obj=from_obj.current).where(expr).alias()
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
    expr, inner_from_obj = expr_to_sql(rel.expr, from_obj=from_obj)
    from_obj = from_obj.replace(current=inner_from_obj.current)
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
    if from_obj.at is not None:
        from_obj, at = from_obj.join_lateral(sel)
    else:
        from_obj, at = from_obj.join_at(sel)
    return at.c.value, from_obj


@expr_to_sql.register
def ExprRecord_to_sql(op: ExprRecord, from_obj):
    args = []
    at = from_obj.at
    for field in op.fields.values():
        args.append(sa.literal(field.name))
        expr, from_obj = expr_to_sql(
            field.expr, from_obj=from_obj.replace(at=at)
        )
        args.append(expr)
    return sa.func.jsonb_build_object(*args), from_obj


@expr_to_sql.register
def ExprColumn_to_sql(op: ExprColumn, from_obj):
    return sa.column(op.column.name, _selectable=from_obj.at), from_obj


@expr_to_sql.register
def ExprIdentity_to_sql(op: ExprIdentity, from_obj):
    pk = []
    for col in op.table.primary_key.columns:
        pk.append(from_obj.at.columns[col.name])
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
