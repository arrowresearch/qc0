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
        queue = [cls]
        found = set()
        while queue:
            cls = queue.pop()
            if cls in found:
                continue
            found.add(cls)
            queue = cls.__subclasses__() + queue

        return {
            cls.name: cls for cls in found if cls.name is not NotImplemented
        }

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
            assert True or isinstance(arg, arg_type), (
                f"{cls.name}(..): expected argument {n + 1}"
                f" to be of type {arg_type.__name__},"
                f" but got {arg.__class__.__name__} instead"
            )


class SelectSig(Sig):
    name = "select"


class AroundSig(Sig):
    name = "around"


class FilterSig(Sig):
    name = "filter"


class GroupSig(Sig):
    name = "group"


class TakeSig(Sig):
    name = "take"


class FirstSig(Sig):
    name = "first"


class SortSig(Sig):
    name = "sort"


class AggrSig(Sig):
    func = None
    unit = NotImplemented

    def compile(self, args):
        func = getattr(sa.func, self.func or self.name)
        return func(*args)


class JsonAggSig(AggrSig):
    name = "jsonb_agg"
    unit = sa.func.cast(sa.literal("[]"), sa.dialects.postgresql.JSONB())


class CountSig(AggrSig):
    name = "count"
    unit = sa.literal(0)


class SumSig(AggrSig):
    name = "sum"
    unit = sa.literal(0)


class AvgSig(AggrSig):
    name = "avg"
    unit = sa.literal(0)


class MinSig(AggrSig):
    name = "min"
    unit = sa.literal(0)


class MaxSig(AggrSig):
    name = "max"
    unit = sa.literal(0)


class ExistsSig(AggrSig):
    name = "exists"
    func = "bool_and"
    unit = sa.literal(False)

    def compile(self, args):
        return sa.func.bool_and(True)


class FuncSig(Sig):
    def compile(self, expr, args):
        func = getattr(sa.func, self.name)
        return func(expr, *args)


class BinOpSig(Sig):
    def compile(self, a, b):
        raise NotImplementedError()


class EqSig(BinOpSig):
    name = "__eq__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a == b)


class NeSig(BinOpSig):
    name = "__ne__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a != b)


class LtSig(BinOpSig):
    name = "__lt__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a < b)


class GtSig(BinOpSig):
    name = "__gt__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a > b)


class LeSig(BinOpSig):
    name = "__le__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a <= b)


class GeSig(BinOpSig):
    name = "__ge__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a >= b)


class AddSig(BinOpSig):
    name = "__add__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a + b)


class SubSig(BinOpSig):
    name = "__sub__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a - b)


class MulSig(BinOpSig):
    name = "__mul__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a * b)


class TruedivSig(BinOpSig):
    name = "__truediv__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a / b)


class AndSig(BinOpSig):
    name = "__and__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a & b)


class OrSig(BinOpSig):
    name = "__or__"
    args = (Expr, Expr)
    compile = classmethod(lambda cls, a, b: a | b)


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


class LikeSig(FuncSig):
    name = "like"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.like(args[0]))


class IlikeSig(FuncSig):
    name = "ilike"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.ilike(args[0]))


class MatchesSig(FuncSig):
    name = "matches"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.op("~")(args[0]))


class ImatchesSig(FuncSig):
    name = "imatches"
    args = (Expr,)
    compile = classmethod(lambda cls, expr, args: expr.op("~*")(args[0]))


class NotSig(FuncSig):
    name = "__not__"
    args = ()
    compile = classmethod(lambda cls, expr, args: sa.not_(expr))
