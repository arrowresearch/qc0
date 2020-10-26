import sqlalchemy as sa

from .base import cached
from .op import Expr


class Sig:
    """ Query Combinator signature."""

    name = NotImplemented
    parent = NotImplemented
    args = NotImplemented

    @classmethod
    @cached
    def registry(cls):
        candiates = cls.__subclasses__()
        return {c.name: c for c in candiates if c.name is not NotImplemented}

    @classmethod
    def get(cls, name):
        return cls.registry().get(name)

    @classmethod
    def validate(cls, args):
        assert len(args) == len(cls.args), (
            f"{cls.name}(..): expected {len(args)} arguments"
            f" got {len(args)} arguments instead"
        )
        for n, (arg, arg_type) in enumerate(zip(args, cls.args)):
            assert isinstance(arg, arg_type), (
                f"{cls.name}(..): expected argument {n + 1}"
                f" to be of type {arg_type.__name__},"
                f" but got {arg.__class__.__name__} instead"
            )


class FuncSig(Sig):
    @classmethod
    def compile(cls, expr, args):
        func = getattr(sa.func, cls.name)
        return func(expr, *args)


class EqSig(FuncSig):
    name = "__eq__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] == args[1])


class NeSig(FuncSig):
    name = "__ne__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] != args[1])


class AddSig(FuncSig):
    name = "__add__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] + args[1])


class SubSig(FuncSig):
    name = "__sub__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] - args[1])


class MulSig(FuncSig):
    name = "__mul__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] * args[1])


class TruedivSig(FuncSig):
    name = "__truediv__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] / args[1])


class AndSig(FuncSig):
    name = "__and__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] & args[1])


class OrSig(FuncSig):
    name = "__or__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, expr, args: args[0] | args[1])


class LengthSig(FuncSig):
    name = "length"
    args = ()


class SubstringSig(FuncSig):
    name = "substring"
    args = (Expr, Expr)


class UpperSig(FuncSig):
    name = "upper"
    args = ()


class LowerSig(FuncSig):
    name = "lower"
    args = ()


class Like(FuncSig):
    name = "like"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.like(args[0]))


class Ilike(FuncSig):
    name = "ilike"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.ilike(args[0]))


class Matches(FuncSig):
    name = "matches"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.op("~")(args[0]))


class Imatches(FuncSig):
    name = "imatches"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.op("~*")(args[0]))