from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as sa_pg
from .syn import (
    Syn,
    Nav,
    Apply,
    Compose,
    Select,
    BinOp,
    Literal,
    Field,
    make_value,
)
from .scope import Cardinality
from .syn_to_op import syn_to_op
from .op_to_sql import op_to_sql

__all__ = ("Q",)


class Q:
    """ Convenience to build syntax trees in Python."""

    def __init__(self, meta: sa.MetaData, engine: sa.engine.Engine, syn=None):
        self.meta = meta
        self.engine = engine
        self.syn = syn

    def s(self, syn):
        return self.__class__(meta=self.meta, engine=self.engine, syn=syn)

    def run(self):
        op = syn_to_op(self.syn, self.meta)
        sql = op_to_sql(op)
        with self.engine.connect() as conn:
            res = conn.execute(sql)
            value = [row.value for row in res.fetchall()]
            if op.card != Cardinality.SEQ:
                value = value[0]
            return value

    def print_syn(self):
        print(self.syn)

    def print_op(self):
        op = syn_to_op(self.syn, self.meta)
        print(op)

    def sql(self, format=False):
        op = syn_to_op(self.syn, self.meta)
        sql = op_to_sql(op)
        sql = sql.compile(self.engine, compile_kwargs={"literal_binds": True})
        sql = str(sql).strip()
        sql = "\n".join([line.strip() for line in sql.split("\n")])
        if format:
            import sqlparse

            sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
        return sql

    def print_sql(self, format=True):
        print(self.sql(format=format))

    def __getattr__(self, name):
        nav = Nav(name=name)
        if self.syn is None:
            return self.s(nav)
        else:
            return self.s(Compose(self.syn, nav))

    def select(self, **fields):
        fields = {
            name: Field(syn=to_syn(syn), name=name)
            for name, syn in fields.items()
        }
        return self.s(Compose(self.syn, Select(fields=fields)))

    def group(self, **fields):
        fields = {
            name: Field(syn=to_syn(syn), name=name)
            for name, syn in fields.items()
        }
        return self.s(Compose(self.syn, Apply("group", fields)))

    def val(self, v):
        val = make_value(v)
        if self.syn is None:
            return self.s(val)
        else:
            return self.s(Compose(self.syn, val))

    def json_val(self, v):
        val = Literal(value=v, type=sa_pg.JSONB())
        if self.syn is None:
            return self.s(val)
        else:
            return self.s(Compose(self.syn, val))

    def __eq__(self, o: Q):
        return self.s(BinOp(op="__eq__", a=self.syn, b=to_syn(o)))

    def __ne__(self, o: Q):
        return self.s(BinOp(op="__ne__", a=self.syn, b=to_syn(o)))

    def __add__(self, o: Q):
        return self.s(BinOp(op="__add__", a=self.syn, b=to_syn(o)))

    def __sub__(self, o: Q):
        return self.s(BinOp(op="__sub__", a=self.syn, b=to_syn(o)))

    def __mul__(self, o: Q):
        return self.s(BinOp(op="__mul__", a=self.syn, b=to_syn(o)))

    def __truediv__(self, o: Q):
        return self.s(BinOp(op="__truediv__", a=self.syn, b=to_syn(o)))

    def __and__(self, o: Q):
        return self.s(BinOp(op="__and__", a=self.syn, b=to_syn(o)))

    def __or__(self, o: Q):
        return self.s(BinOp(op="__or__", a=self.syn, b=to_syn(o)))

    def __rshift__(self, o: Q):
        return self.s(Compose(a=self.syn, b=to_syn(o)))

    def __call__(self, *args):
        args = tuple(to_syn(arg) for arg in args)
        if isinstance(self.syn, Nav):
            name = self.syn.name
            return self.s(Apply(name=name, args=args))
        elif isinstance(self.syn, Compose) and isinstance(self.syn.b, Nav):
            parent = self.syn.a
            name = self.syn.b.name
            return self.s(Compose(parent, Apply(name=name, args=args)))
        else:
            assert (  # pragma: no cover
                False
            ), "SyntaxError: cannot do call here"


def to_syn(v):
    if isinstance(v, Q):
        return v.syn
    elif isinstance(v, Syn):
        return v
    else:
        return make_value(v)
