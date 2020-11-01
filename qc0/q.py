"""

    qc0.q
    =====

    Python API for querying data

"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as sa_pg

from . import syntax
from .scope import Cardinality
from .plan import plan
from .compile import compile

__all__ = ("Q",)


class Q:
    """ Python API for querying data."""

    def __init__(self, meta: sa.MetaData, engine: sa.engine.Engine, syn=None):
        self.meta = meta
        self.engine = engine
        self.syn = syn

    #
    # Query API
    #

    def nav(self, name):
        """ Navigate to ``name``."""
        nav = syntax.Nav(name=name)
        if self.syn is None:
            return self._make(nav)
        else:
            return self._make(syntax.Compose(self.syn, nav))

    def then(self, o):
        """ Compose with ``o`` query."""
        return self._make(syntax.Compose(a=self.syn, b=to_syn(o)))

    def select(self, **fields):
        fields = {
            name: syntax.Field(syn=to_syn(syn), name=name)
            for name, syn in fields.items()
        }
        syn = syntax.Select(fields=fields)
        if self.syn is not None:
            syn = syntax.Compose(self.syn, syn)
        return self._make(syn)

    def group(self, **fields):
        fields = {
            name: syntax.Field(syn=to_syn(syn), name=name)
            for name, syn in fields.items()
        }
        return self._make(
            syntax.Compose(self.syn, syntax.Apply("group", fields))
        )

    def desc(self):
        """ Compose with ``o`` query."""
        return self._make(syntax.Desc(self.syn))

    def val(self, v):
        val = syntax.make_value(v)
        if self.syn is None:
            return self._make(val)
        else:
            return self._make(syntax.Compose(self.syn, val))

    def json_val(self, v):
        val = syntax.Literal(value=v, type=sa_pg.JSONB())
        if self.syn is None:
            return self._make(val)
        else:
            return self._make(syntax.Compose(self.syn, val))

    def __getattr__(self, name):
        return self.nav(name)

    def __eq__(self, o: Q):
        return self._make(syntax.BinOp(op="__eq__", a=self.syn, b=to_syn(o)))

    def __ne__(self, o: Q):
        return self._make(syntax.BinOp(op="__ne__", a=self.syn, b=to_syn(o)))

    def __lt__(self, o: Q):
        return self._make(syntax.BinOp(op="__lt__", a=self.syn, b=to_syn(o)))

    def __gt__(self, o: Q):
        return self._make(syntax.BinOp(op="__gt__", a=self.syn, b=to_syn(o)))

    def __le__(self, o: Q):
        return self._make(syntax.BinOp(op="__le__", a=self.syn, b=to_syn(o)))

    def __ge__(self, o: Q):
        return self._make(syntax.BinOp(op="__ge__", a=self.syn, b=to_syn(o)))

    def __add__(self, o: Q):
        return self._make(syntax.BinOp(op="__add__", a=self.syn, b=to_syn(o)))

    def __radd__(self, o: Q):
        return self._make(syntax.BinOp(op="__add__", a=to_syn(o), b=self.syn))

    def __sub__(self, o: Q):
        return self._make(syntax.BinOp(op="__sub__", a=self.syn, b=to_syn(o)))

    def __rsub__(self, o: Q):
        return self._make(syntax.BinOp(op="__sub__", a=to_syn(o), b=self.syn))

    def __mul__(self, o: Q):
        return self._make(syntax.BinOp(op="__mul__", a=self.syn, b=to_syn(o)))

    def __rmul__(self, o: Q):
        return self._make(syntax.BinOp(op="__mul__", a=to_syn(o), b=self.syn))

    def __truediv__(self, o: Q):
        return self._make(
            syntax.BinOp(op="__truediv__", a=self.syn, b=to_syn(o))
        )

    def __and__(self, o: Q):
        return self._make(syntax.BinOp(op="__and__", a=self.syn, b=to_syn(o)))

    def __or__(self, o: Q):
        return self._make(syntax.BinOp(op="__or__", a=self.syn, b=to_syn(o)))

    def __invert__(self):
        return self._make(
            syntax.Compose(self.syn, syntax.Apply(name="__not__", args=[]))
        )

    def __rshift__(self, o: Q):
        return self._make(syntax.Compose(a=self.syn, b=to_syn(o)))

    def __call__(self, *args):
        args = tuple(to_syn(arg) for arg in args)
        if isinstance(self.syn, syntax.Nav):
            name = self.syn.name
            return self._make(syntax.Apply(name=name, args=args))
        elif isinstance(self.syn, syntax.Compose) and isinstance(
            self.syn.b, syntax.Nav
        ):
            parent = self.syn.a
            name = self.syn.b.name
            return self._make(
                syntax.Compose(parent, syntax.Apply(name=name, args=args))
            )
        else:
            assert (  # pragma: no cover
                False
            ), "SyntaxError: cannot do call here"

    #
    # Execution API
    #

    def run(self):
        """ Execute query and return result."""
        op = plan(self.syn, self.meta)
        sql = compile(op)
        with self.engine.connect() as conn:
            res = conn.execute(sql)
            value = [row.value for row in res.fetchall()]
            if op.card != Cardinality.SEQ:
                value = value[0]
            return value

    @property
    def sql(self):
        """ Generated SQL query."""
        return self._sql()

    #
    # Private/Debug API
    #

    def print_syn(self):
        """ Print syntax tree structure."""
        print(self.syn)

    def print_op(self):
        """ Print plan structure."""
        op = plan(self.syn, self.meta)
        print(op)

    def print_sql(self, format=True):
        """ Print SQL query."""
        print(self._sql(format=format))

    def _sql(self, format=True):
        """ Get generated SQL query."""
        op = plan(self.syn, self.meta)
        sql = compile(op)
        sql = sql.compile(self.engine, compile_kwargs={"literal_binds": True})
        sql = str(sql).strip()
        sql = "\n".join([line.strip() for line in sql.split("\n")])
        if format:
            import sqlparse

            sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
        return sql

    def _make(self, syn):
        return self.__class__(meta=self.meta, engine=self.engine, syn=syn)


def to_syn(v):
    if isinstance(v, Q):
        return v.syn
    elif isinstance(v, syntax.Syn):
        return v
    else:
        return syntax.make_value(v)
