import dataclasses
import yaml


class StructMeta(type):
    def __new__(mcs, name, bases, fields):
        cls = super().__new__(mcs, name, bases, fields)
        return dataclasses.dataclass(frozen=True)(cls)


class Struct(metaclass=StructMeta):
    def replace(self, **values):
        return dataclasses.replace(self, **values)

    def __yaml__(self):
        fields = {}
        for k in getattr(self.__class__, "__annotations__", []):
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
