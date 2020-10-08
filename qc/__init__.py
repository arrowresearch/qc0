from .syn import q
from .bind import bind
from .compile import compile

__version__ = "0.1.0"

__all__ = ("parse", "q", "bind", "compile")


def parse(q):
    """ Parse query into a syntax tree."""
    raise NotImplementedError()
