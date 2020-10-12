from .syn import Q, q
from .syn_to_op import syn_to_op
from .op_to_sql import op_to_sql

__version__ = "0.1.0"

__all__ = ("parse", "Q", "q", "syn_to_op", "op_to_sql")


def parse(q):
    """ Parse query into a syntax tree."""
    raise NotImplementedError()  # pragma: no cover
