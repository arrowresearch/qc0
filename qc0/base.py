import dataclasses
import collections.abc
import yaml
import functools
import typing


class StructMeta(type):
    def __new__(mcs, name, bases, fields):
        cls = super().__new__(mcs, name, bases, fields)
        return dataclasses.dataclass(frozen=True)(cls)


class Struct(metaclass=StructMeta):
    def replace(self, **values):
        return dataclasses.replace(self, **values)

    def __post_init__(self):
        errors = []
        for k, t in typing.get_type_hints(self.__class__).items():
            v = getattr(self, k)
            errors = errors + check(t, v, f"key `{k}` ")
        if errors:
            errors = [
                f"Unable to create `{self.__class__.__name__}` struct:"
            ] + errors
            raise TypeError("\n".join(errors))

    def __yaml__(self):
        fields = {}
        for k in typing.get_type_hints(self.__class__):
            v = getattr(self, k)
            if isinstance(v, Struct):
                v = v
            elif type(v) in (dict, list, int, str, bool, tuple):
                v = v
            else:
                v = str(v)
            fields[k] = v
        return fields

    def __str__(self):
        return yaml.dump(self)


def check(t, v, prefix):
    errors = []
    t_orig = getattr(t, "__origin__", None)
    if t is typing.Any:
        return errors
    if t_orig is dict:
        if not isinstance(v, dict):
            errors.append(f"{prefix}expected `{t}` received `{type(v)}`")
            return errors
        kt, vt = t.__args__
        for k, kv in v.items():
            errors = (
                errors
                + check(kt, k, f"{prefix}key `{k}` ")
                + check(vt, kv, f"{prefix}value at `{k}` ")
            )
        return errors
    if t_orig is list:
        if not isinstance(v, (list, tuple)):
            errors.append(f"{prefix}expected `{t}` received `{type(v)}`")
        (vt,) = t.__args__
        for idx, iv in enumerate(v):
            errors = errors + check(vt, iv, f"{prefix}value at `{idx}` ")
        return errors
    if t_orig is typing.Union:
        for a in t.__args__:
            a_errors = check(a, v, prefix)
            if not a_errors:
                return []
            errors = errors + a_errors
        return errors
    if t_orig is collections.abc.Callable:
        if not callable(v):
            errors.append(f"{prefix}expected `{t}` received `{type(v)}`")
        return errors
    if not isinstance(v, t):
        errors.append(f"{prefix}expected `{t}` received `{type(v)}`")
        return errors
    return errors


def Struct_representer(dumper, self):
    fields = self.__yaml__()
    return dumper.represent_mapping(
        f"!{self.__class__.__name__}", list(fields.items())
    )


yaml.add_multi_representer(Struct, Struct_representer)


def cached(f):
    return functools.lru_cache(maxsize=None, typed=True)(f)


undefined = object()
