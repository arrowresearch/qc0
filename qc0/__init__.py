import sqlalchemy as sa

from .syn import Q, q
from .syn_to_op import syn_to_op
from .op_to_sql import op_to_sql
from .scope import Cardinality

__version__ = "0.1.0"

__all__ = ("execute", "parse", "Q", "q", "syn_to_op", "op_to_sql")


def execute(query, meta: sa.MetaData, engine: sa.engine.Engine):
    """ Execute query."""
    if isinstance(query, Q):
        query = query.syn
    elif isinstance(query, str):
        query = parse(query)
    op = syn_to_op(query, meta)
    sql = op_to_sql(op)
    with engine.connect() as conn:
        res = conn.execute(sql)
        value = [row.value for row in res.fetchall()]
        if op.card != Cardinality.SEQ:
            value = value[0]
        return value


def parse(q):
    """ Parse query into a syntax tree."""
    raise NotImplementedError()  # pragma: no cover
