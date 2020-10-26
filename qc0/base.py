import dataclasses
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


def Struct_representer(dumper, self):
    fields = self.__yaml__()
    return dumper.represent_mapping(
        f"!{self.__class__.__name__}", list(fields.items())
    )


yaml.add_multi_representer(Struct, Struct_representer)


def cached(f):
    return functools.lru_cache(maxsize=None, typed=True)(f)


undefined = object()
