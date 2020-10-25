import sqlalchemy as sa

from .base import cached
from .op import Expr
from .scope import Cardinality


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
        def validate_shape(name, op, shape):
            typ, card = shape
            assert isinstance(op, typ), (
                f"{cls.name}(..): expected {name}"
                f" to be of type {typ.__name__},"
                f" but got {op.__class__.__name__} instead"
            )
            assert op.card in card, (
                f"{cls.name}(..): expected {name}"
                f" to have {card} cardinality"
                f" but got {op.card} cardinality instead"
            )

        assert len(args) == len(cls.args), (
            f"{cls.name}(..): expected {len(args)} arguments"
            f" got {len(args)} arguments instead"
        )
        for n, (arg, arg_shape) in enumerate(zip(args, cls.args)):
            validate_shape(f"argument #{n + 1}", arg, arg_shape)


class FuncSig(Sig):
    @classmethod
    def compile(cls, expr, args):
        func = getattr(sa.func, cls.name)
        return func(expr, *args)


class SubstringSig(FuncSig):
    name = "substring"
    args = (
        (Expr, {Cardinality.ONE}),
        (Expr, {Cardinality.ONE}),
    )


class UpperSig(FuncSig):
    name = "upper"
    args = ()


class LowerSig(FuncSig):
    name = "lower"
    args = ()


class Like(FuncSig):
    name = "like"
    args = ((Expr, {Cardinality.ONE}),)

    @classmethod
    def compile(cls, expr, args):
        (pattern,) = args
        return expr.like(pattern)


class Ilike(FuncSig):
    name = "ilike"
    args = ((Expr, {Cardinality.ONE}),)

    @classmethod
    def compile(cls, expr, args):
        (pattern,) = args
        return expr.ilike(pattern)


class Matches(FuncSig):
    name = "matches"
    args = ((Expr, {Cardinality.ONE}),)

    @classmethod
    def compile(cls, expr, args):
        (pattern,) = args
        return expr.op("~")(pattern)


class Imatches(FuncSig):
    name = "imatches"
    args = ((Expr, {Cardinality.ONE}),)

    @classmethod
    def compile(cls, expr, args):
        (pattern,) = args
        return expr.op("~*")(pattern)
