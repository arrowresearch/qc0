from .syn import q, literal
from .bind import bind
from .compile import compile

__version__ = "0.1.0"

__all__ = ("parse", "q", "bind", "compile", "literal")


def parse(q):
    """ Parse query into a syntax tree."""
    raise NotImplementedError()
