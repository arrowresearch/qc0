from typing import Dict, List, Any
from functools import singledispatch
import collections
import sqlalchemy as sa
from sqlalchemy.sql.selectable import Selectable, Join, Alias
from .base import Struct
from .op import (
    Op,
    Rel,
    RelVoid,
    RelTable,
    RelJoin,
    RelRevJoin,
    RelParent,
    RelAggregateParent,
    RelTake,
    RelFilter,
    RelSort,
    RelGroup,
    RelAroundParent,
    Expr,
    ExprOp,
    ExprRecord,
    ExprColumn,
    ExprCompute,
    ExprIdentity,
    ExprConst,
    ExprApply,
)


def compile(op: Op):
    """ Compile operations into SQL."""
    value, from_obj = op_to_sql(op, from_obj=From.make(None), ns=Namespace())
    return from_obj.to_select(value)


class Namespace(collections.abc.Mapping):
    def __init__(self, ns=None):
        self.ns = {**(ns or {})}

    def __setitem__(self, name, expr):
        self.ns[name] = expr

    def __getitem__(self, name):
        return self.ns[name]

    def __iter__(self):
        return iter(self.ns)

    def __len__(self):
        return len(self.ns)

    def __add__(self, o):
        return Namespace({**self.ns, **o})

    def __radd__(self, o):
        return Namespace({**o, **self.ns})

    def __str__(self):
        return str(self.ns)

    def __repr__(self):
        return repr(self.ns)

    def rebase(self, from_obj):
        return Namespace({name: from_obj.at.columns[name] for name in self})

    def copy(self):
        return Namespace(self.ns)


class From(Struct):
    existing: Dict[any, Selectable]
    current: Selectable
    at: Selectable
    where: Any = None
    limit: Any = None
    order: Any = None
    group_by_columns: List[str] = None
    correlate: Any = None

    def __post_init__(self):
        if self.group_by_columns is None:
            object.__setattr__(self, "group_by_columns", ())

    def join_at(
        self, from_obj, *by, outer=False, lateral=False, navigation=False
    ):
        if self.current is None:
            assert not by
            from_obj = from_obj.alias()

            return self.make(from_obj, correlate=self.correlate), from_obj

        if self.limit is not None and navigation:
            self = self.make(
                self.to_select(None).alias(),
                correlate=self.correlate,
            )

        # NOTE(andreypopp): this is a hacky way to dedup existing, need to consider
        # another approach based on structural query equality...
        key = (self.at.element if self.at is not None else None, from_obj, by)
        if key in self.existing:
            at = self.existing[key]
            return self.replace(at=at), at

        if lateral:
            at = from_obj.lateral()
        else:
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

    def add_where(self, where):
        where = (self.where & where) if self.where is not None else where
        return self.replace(where=where)

    def add_limit(self, limit):
        return self.replace(limit=limit)

    def add_order(self, order):
        return self.replace(order=order)

    @classmethod
    def make(cls, from_obj=None, where=None, correlate=None):
        assert not isinstance(from_obj, Join)
        if from_obj is not None and not isinstance(from_obj, Alias):
            from_obj = from_obj.alias()
        return cls(
            current=from_obj,
            at=from_obj,
            existing={},
            where=where,
            correlate=correlate,
        )

    def to_select(self, value):
        cols = [*self.group_by_columns]
        if value is not None:
            cols.append(value.label("value"))
        else:
            cols.append(self.at)

        sel = sa.select(cols, from_obj=self.current)
        if self.correlate is not None:
            sel = sel.correlate(self.correlate)

        if self.where is not None:
            sel = sel.where(self.where)
        if self.order is not None:
            sel = sel.order_by(*self.order)
        if self.limit is not None:
            sel = sel.limit(self.limit)
        return sel


def op_to_sql(op: Op, from_obj, ns):
    expr = None
    inner_from_obj, ns = rel_to_sql(op.rel, from_obj=from_obj, ns=ns)
    if op.expr is not None:
        expr, inner_from_obj = expr_to_sql(
            op.expr,
            from_obj=inner_from_obj,
            ns=ns,
        )

    if op.sig is None:
        return expr, inner_from_obj

    if inner_from_obj.limit is not None or inner_from_obj.order is not None:
        inner_from_obj = From.make(
            inner_from_obj.to_select(expr).alias(),
            correlate=inner_from_obj.correlate,
        )
        value = sa.func.coalesce(
            op.sig.compile([inner_from_obj.at.c.value]),
            op.sig.unit,
        )
    else:
        value = sa.func.coalesce(
            op.sig.compile([expr]),
            op.sig.unit,
        )
    sel = inner_from_obj.to_select(value).alias()
    if from_obj.at is not None:
        from_obj, at = from_obj.join_lateral(sel)
    else:
        from_obj, at = from_obj.join_at(sel)
    return at.c.value, from_obj


@singledispatch
def rel_to_sql(rel: Rel, from_obj, ns):
    raise NotImplementedError(  # pragma: no cover
        f"rel_to_sql({type(rel).__name__})"
    )


@rel_to_sql.register
def RelVoid_to_sql(rel: RelVoid, from_obj, ns):
    next_ns = Namespace()
    if rel.compute:
        at = from_obj.at
        for field in rel.compute:
            expr, from_obj = op_to_sql(field.op, from_obj.replace(at=at), ns)
            next_ns[field.name] = expr
        from_obj = from_obj.replace(at=at)
    return from_obj, ns + next_ns


@rel_to_sql.register
def RelTable_to_sql(rel: RelTable, from_obj, ns):
    from_obj, ns = rel_to_sql(rel.rel, from_obj, ns)
    from_obj = From.make(rel.table)

    next_ns = Namespace()
    if rel.compute:
        at = from_obj.at
        for field in rel.compute:
            expr, from_obj = op_to_sql(field.op, from_obj.replace(at=at), ns)
            next_ns[field.name] = expr
        from_obj = from_obj.replace(at=at)
    return from_obj, ns + next_ns


@rel_to_sql.register
def RelJoin_to_sql(rel: RelJoin, from_obj, ns):
    if isinstance(rel.rel, RelAroundParent):
        table = rel.fk.column.table
        at = from_obj.at
        from_obj = From.make(
            table.select()
            .correlate(at)
            .where(
                table.columns[rel.fk.column.name]
                == from_obj.at.columns[rel.fk.parent.name]
            )
        )
        from_obj = from_obj.replace(correlate=at)
    else:
        from_obj, ns = rel_to_sql(rel.rel, from_obj=from_obj, ns=ns)
        from_obj, _ = from_obj.join_at(
            rel.fk.column.table,
            (rel.fk.parent.name, rel.fk.column.name),
            navigation=not isinstance(rel.rel, RelParent),
        )

    next_ns = Namespace()
    if rel.compute:
        at = from_obj.at
        for field in rel.compute:
            expr, from_obj = op_to_sql(field.op, from_obj.replace(at=at), ns)
            next_ns[field.name] = expr
        from_obj = from_obj.replace(at=at)
    return from_obj, ns + next_ns


@rel_to_sql.register
def RelRevJoin_to_sql(rel: RelRevJoin, from_obj, ns):
    if isinstance(rel.rel, RelParent):
        table = rel.fk.parent.table.alias()
        from_obj = From.make(
            table,
            where=table.columns[rel.fk.parent.name]
            == from_obj.at.columns[rel.fk.column.name],
            correlate=from_obj.at,
        )
    else:
        from_obj, ns = rel_to_sql(rel.rel, from_obj=from_obj, ns=ns)
        from_obj, _ = from_obj.join_at(
            rel.fk.parent.table,
            (rel.fk.column.name, rel.fk.parent.name),
            navigation=True,
        )

    next_ns = Namespace()
    if rel.compute:
        at = from_obj.at
        for field in rel.compute:
            expr, from_obj = op_to_sql(field.op, from_obj.replace(at=at), ns)
            next_ns[field.name] = expr
        from_obj = from_obj.replace(at=at)
    return from_obj, ns + next_ns


@rel_to_sql.register
def RelTake_to_sql(rel: RelTake, from_obj, ns):
    from_obj, ns = rel_to_sql(rel.rel, from_obj, ns)
    # If we have already LIMIT set we need to produce wrap this FROM as a
    # subselect with another LIMIT.
    if from_obj.limit is not None:
        from_obj = From.make(
            from_obj.to_select(None).alias(),
            correlate=from_obj.correlate,
        )
        ns = ns.rebase(from_obj)
    at = from_obj.at
    take, from_obj = op_to_sql(rel.take, from_obj, ns)
    from_obj = from_obj.replace(at=at)
    from_obj = from_obj.add_limit(take)
    return from_obj, ns


@rel_to_sql.register
def RelSort_to_sql(rel: RelSort, from_obj, ns):
    from_obj, ns = rel_to_sql(rel.rel, from_obj, ns)
    # If we have already LIMIT set we need to produce wrap this FROM as a
    # subselect with another LIMIT.
    if from_obj.limit is not None or from_obj.order is not None:
        from_obj = From.make(
            from_obj.to_select(None).alias(),
            correlate=from_obj.correlate,
        )
        ns = ns.rebase(from_obj)
    at = from_obj.at
    order_by = []
    for sort in rel.sort:
        col, from_obj = op_to_sql(sort.op, from_obj.replace(at=at), ns)
        if sort.desc:
            col = col.desc()
        order_by.append(col)
    from_obj = from_obj.replace(at=at)
    from_obj = from_obj.add_order(order_by)
    return from_obj, ns


@rel_to_sql.register
def RelFilter_to_sql(rel: RelFilter, from_obj, ns):
    from_obj, ns = rel_to_sql(rel.rel, from_obj, ns)
    # If we have already LIMIT set we need to produce wrap this FROM as a
    # subselect with WHERE.
    if from_obj.limit is not None:
        from_obj = From.make(
            from_obj.to_select(None).alias(),
            correlate=from_obj.correlate,
        )
        ns = ns.rebase(from_obj)
    at = from_obj.at
    cond, from_obj = op_to_sql(rel.cond, from_obj, ns)
    from_obj = from_obj.replace(at=at)
    from_obj = from_obj.add_where(cond)
    return from_obj, ns


@rel_to_sql.register
def RelParent_to_sql(rel: RelParent, from_obj, ns):
    return from_obj, ns


@rel_to_sql.register
def RelAggregateParent_to_sql(rel: RelAggregateParent, from_obj, ns):
    return from_obj, ns


@rel_to_sql.register
def RelGroup_to_sql(rel: RelGroup, from_obj, ns):
    from_obj, ns = rel_to_sql(rel.rel, from_obj=from_obj, ns=ns)

    # TODO(andreypopp): might need to convert this to CTE for complex
    # expressions
    initial_from_obj = from_obj

    def build_kernel():
        from_obj = initial_from_obj
        at = from_obj.at
        columns = []
        for field in rel.fields.values():
            expr, from_obj = op_to_sql(
                field.op,
                from_obj=from_obj.replace(at=at),
                ns=ns,
            )
            columns.append(expr.label(field.name))
        columns = from_obj.group_by_columns + tuple(columns)
        return columns, from_obj.replace(at=at, group_by_columns=columns)

    columns, from_obj = build_kernel()

    next_ns = Namespace()
    for field in rel.compute:
        op = field.op
        if op.sig is not None:
            continue
        value = None
        expr, from_obj = op_to_sql(op, from_obj=from_obj, ns=ns)
        expr = expr.label(field.name)
        next_ns[field.name] = expr
        columns = columns + (expr,)

    ns = ns + next_ns

    if columns:
        sel = sa.select(
            [*from_obj.group_by_columns, *columns], from_obj=from_obj.current
        )
        if from_obj.where is not None:
            sel = sel.where(from_obj.where)
        sel = sel.group_by(*from_obj.group_by_columns).alias()
        from_obj = From.make(sel)
        ns = ns.rebase(from_obj)
    else:
        sel = sa.select(
            [*from_obj.group_by_columns],
            from_obj=sa.select([sa.literal(1)]).alias(),
        )
        from_obj = From.make(sel)
        ns = ns.rebase(from_obj)

    if not any(f.op.sig is not None for f in rel.compute):
        return from_obj, ns

    next_ns = Namespace()
    result_columns = [from_obj.current.columns[c.name] for c in tuple(columns)]
    for field in rel.compute:
        op = field.op
        if op.sig is None:
            continue
        assert op.sig is not None
        columns, kernel = build_kernel()

        value = None
        inner_from_obj, inner_ns = rel_to_sql(
            op.rel,
            from_obj=kernel,
            ns=ns,
        )
        if op.expr is not None:
            value, inner_from_obj = expr_to_sql(
                op.expr,
                from_obj=inner_from_obj,
                ns=ns + inner_ns,
            )

        value = op.sig.compile([value])
        if kernel.current in inner_from_obj.current._from_objects:
            cols = columns
        else:
            cols = [inner_from_obj.current.columns[c.name] for c in columns]
        inner_sel = sa.select(
            [*cols, value.label("value")],
            from_obj=inner_from_obj.current,
        ).group_by(*cols)
        if inner_from_obj.where is not None:
            inner_sel = inner_sel.where(inner_from_obj.where)
        inner_sel = inner_sel.alias()
        from_obj, inner_at = from_obj.join_at(
            inner_sel,
            *((c.name, c.name) for c in columns),
            outer=True,
            lateral=True,
        )
        expr = sa.func.coalesce(inner_at.c.value, op.sig.unit).label(
            field.name
        )
        next_ns[field.name] = expr
        result_columns.append(expr)

    from_obj = From.make(
        sa.select(result_columns, from_obj=from_obj.current).alias()
    )
    ns = ns + next_ns
    ns = ns.rebase(from_obj)
    return from_obj, ns


@singledispatch
def expr_to_sql(expr: Expr, from_obj, ns):
    raise NotImplementedError(  # pragma: no cover
        f"expr_to_sql({type(expr).__name__})"
    )


@expr_to_sql.register
def ExprOp_to_sql(expr: ExprOp, from_obj, ns):
    return op_to_sql(expr.op, from_obj, ns)


@expr_to_sql.register
def ExprRecord_to_sql(op: ExprRecord, from_obj, ns):
    args = []
    at = from_obj.at
    for field in op.fields.values():
        args.append(sa.literal(field.name))
        expr, from_obj = op_to_sql(
            field.op,
            from_obj=from_obj.replace(at=at),
            ns=ns,
        )
        args.append(expr)
    return sa.func.jsonb_build_object(*args), from_obj


@expr_to_sql.register
def ExprColumn_to_sql(expr: ExprColumn, from_obj, ns):
    return from_obj.at.columns[expr.column.name], from_obj


@expr_to_sql.register
def ExprCompute_to_sql(expr: ExprCompute, from_obj, ns):
    name = expr.name
    expr = ns.get(name)
    assert expr is not None, f"missing {name}"
    return expr, from_obj


@expr_to_sql.register
def ExprIdentity_to_sql(op: ExprIdentity, from_obj, ns):
    pk = []
    for col in op.table.primary_key.columns:
        pk.append(from_obj.at.columns[col.name])
    return sa.cast(sa.func.row(*pk), sa.String()), from_obj


@expr_to_sql.register
def ExprConst_to_sql(op: ExprConst, from_obj, ns):
    return op.embed(op.value), from_obj


@expr_to_sql.register
def ExprApply_to_sql(op: ExprApply, from_obj, ns):
    if op.expr is not None:
        parent, from_obj = expr_to_sql(op.expr, from_obj, ns)
    else:
        parent = None
    at = from_obj.at
    args = []
    for arg in op.args:
        expr, from_obj = expr_to_sql(arg, from_obj.replace(at=at), ns)
        args.append(expr)
    from_obj = from_obj.replace(at=at)
    expr = op.compile(parent, args)
    return expr, from_obj
