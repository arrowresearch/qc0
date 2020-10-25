from typing import Dict, List
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
    RelAggregateParent,
    RelExpr,
    RelTake,
    RelFilter,
    RelGroup,
    Expr,
    ExprRel,
    ExprAggregateRel,
    ExprRecord,
    ExprColumn,
    ExprIdentity,
    ExprConst,
    ExprBinOp,
    ExprApply,
    ExprRaw,
)


def op_to_sql(op):
    """ Compile operations into SQL."""
    from_obj = From.make(None)
    if isinstance(op, Rel):
        value, from_obj = rel_to_sql(op, from_obj)
        return from_obj.to_select(value)
    elif isinstance(op, Expr):
        value, from_obj = expr_to_sql(op, from_obj)
        return from_obj.to_select(value)
    else:
        assert False  # pragma: no cover


class From(Struct):
    existing: Dict[any, Selectable]
    current: Selectable
    at: Selectable
    group_by_columns: List[str] = None

    def __post_init__(self):
        if self.group_by_columns is None:
            object.__setattr__(self, "group_by_columns", ())

    def join_at(self, from_obj, *by, outer=False):
        if self.current is None:
            assert not by
            from_obj = from_obj.alias()

            return self.make(from_obj), from_obj

        # NOTE(andreypopp): this is a hacky way to dedup existing, need to consider
        # another approach based on structural query equality...
        key = (self.at.element if self.at is not None else None, from_obj, by)
        if key in self.existing:
            at = self.existing[key]
            return self.replace(at=at), at

        at = from_obj.alias()
        if by:
            (left, right), *rest = by
            condition = self.at.columns[left] == at.columns[right]
            for left, right in rest:
                condition = condition & (
                    self.at.columns[left] == at.columns[right]
                )
        else:
            condition = sa.true()
        join = sa.outerjoin if outer else sa.join
        current = join(self.current, at, condition)
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
    def make(cls, from_obj=None):
        assert not isinstance(from_obj, Join)
        if from_obj is not None and not isinstance(from_obj, Alias):
            from_obj = from_obj.alias()
        return From(current=from_obj, at=from_obj, existing={})

    def to_select(self, value):
        from_obj = self.current
        if from_obj is None:
            return sa.select([*self.group_by_columns, value.label("value")])
        elif value is None:
            return from_obj.select()
        else:
            return sa.select(
                [*self.group_by_columns, value.label("value")],
                from_obj=from_obj,
            )


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
    # TODO(andreypopp): try to uncomment and see why/if we have failures
    # assert from_obj.current is None
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
    elif isinstance(rel.rel, RelAggregateParent):
        value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj)
        table = rel.fk.parent.table
        from_obj, _ = from_obj.join_at(
            table, (rel.fk.column.name, rel.fk.parent.name)
        )
        return value, from_obj
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
        sa.select(
            [*from_obj.group_by_columns, from_obj.at],
            from_obj=from_obj.current,
        )
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
    sel = (
        sa.select(
            [*from_obj.group_by_columns, prev_at], from_obj=from_obj.current
        )
        .where(expr)
        .alias()
    )
    from_obj = From.make(sel)
    return val, from_obj


@rel_to_sql.register
def RelParent_to_sql(rel: RelParent, from_obj):
    return None, from_obj


@rel_to_sql.register
def RelAggregateParent_to_sql(rel: RelAggregateParent, from_obj):
    return None, from_obj


@rel_to_sql.register
def RelExpr_to_sql(rel: RelExpr, from_obj):
    value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj)
    # assert value is None
    expr, from_obj = expr_to_sql(rel.expr, from_obj=from_obj)
    return expr, from_obj


@rel_to_sql.register
def RelGroup_to_sql(rel: RelGroup, from_obj):
    value, from_obj = rel_to_sql(rel.rel, from_obj=from_obj)
    assert value is None

    # TODO(andreypopp): might need to convert this to CTE for complex
    # expressions
    initial_from_obj = from_obj

    def build_kernel():
        from_obj = initial_from_obj
        at = from_obj.at
        columns = []
        for field in rel.fields.values():
            expr, from_obj = expr_to_sql(
                field.expr,
                from_obj=from_obj.replace(at=at),
            )
            columns.append(expr.label(field.name))
        columns = from_obj.group_by_columns + tuple(columns)
        return columns, from_obj.replace(at=at, group_by_columns=columns)

    columns, from_obj = build_kernel()
    sel = (
        sa.select(
            [*from_obj.group_by_columns, *columns], from_obj=from_obj.current
        )
        .group_by(*from_obj.group_by_columns)
        .alias()
    )
    from_obj = From.make(sel)

    if not rel.aggregates:
        return None, from_obj

    result_columns = [from_obj.current.columns[c.name] for c in tuple(columns)]
    for name, expr in rel.aggregates.items():
        columns, kernel = build_kernel()
        value, inner_from_obj = rel_to_sql(expr.rel, from_obj=kernel)
        # aggregate
        if expr.func is None:
            value = sa.func.jsonb_agg(value)
        else:
            value = getattr(sa.func, expr.func)(value)

        if kernel.current in inner_from_obj.current._from_objects:
            cols = columns
        else:
            cols = [inner_from_obj.current.columns[c.name] for c in columns]
        inner_sel = (
            sa.select(
                [*cols, value.label("value")],
                from_obj=inner_from_obj.current,
            )
            .group_by(*cols)
            .alias()
        )
        from_obj, inner_at = from_obj.join_at(
            inner_sel, *((c.name, c.name) for c in columns), outer=True
        )
        result_columns.append(
            sa.func.coalesce(inner_at.c.value, expr.unit).label(name)
        )

    from_obj = From.make(
        sa.select(result_columns, from_obj=from_obj.current).alias()
    )
    return None, from_obj


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
    sel = inner_from_obj.to_select(value)
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
def ExprRaw_to_sql(op: ExprRaw, from_obj):
    return op.raw, from_obj


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
def ExprApply_to_sql(op: ExprApply, from_obj):
    parent, from_obj = expr_to_sql(op.expr, from_obj)
    at = from_obj.at
    args = []
    for arg in op.args:
        expr, from_obj = expr_to_sql(arg, from_obj.replace(at=at))
        args.append(expr)
    from_obj = from_obj.replace(at=at)
    expr = op.compile(parent, args)
    return expr, from_obj


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
