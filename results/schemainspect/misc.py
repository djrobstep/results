import inspect
from reprlib import recursive_repr

# from pkg_resources import resource_stream as pkg_resource_stream

from importlib.resources import files


def connection_from_s_or_c(s_or_c):  # pragma: no cover
    if hasattr(s_or_c, "s"):
        s_or_c = s_or_c.s

    try:
        s_or_c.engine
        return s_or_c

    except AttributeError:
        try:
            return s_or_c.connection()
        except (AttributeError, TypeError):
            return s_or_c


class AutoRepr:  # pragma: no cover
    @recursive_repr()
    def __repr__(self):
        done = set()

        cname = self.__class__.__name__

        vals = []
        for k in sorted(dir(self)):
            v = getattr(self, k)

            if not k.startswith("_") and (not callable(v)) and id(v) not in done:
                done.add(id(v))

                attr = "{}={}".format(k, repr(v))

                vals.append(attr)
        return "{}({})".format(cname, ", ".join(vals))

    def __str__(self):
        return repr(self)

    def __ne__(self, other):
        return not self == other


def unquoted_identifier(identifier, *, schema=None, identity_arguments=None):
    if identifier is None and schema is not None:
        return schema
    s = "{}".format(identifier)
    if schema:
        s = "{}.{}".format(schema, s)
    if identity_arguments is not None:
        s = "{}({})".format(s, identity_arguments)
    return s


def quoted_identifier(identifier, schema=None, identity_arguments=None):
    if identifier is None and schema is not None:
        return '"{}"'.format(schema.replace('"', '""'))
    s = '"{}"'.format(identifier.replace('"', '""'))
    if schema:
        s = '"{}".{}'.format(schema.replace('"', '""'), s)
    if identity_arguments is not None:
        s = "{}({})".format(s, identity_arguments)
    return s


def external_caller():
    i = inspect.stack()
    names = (inspect.getmodule(i[x][0]).__name__ for x in range(len(i)))
    return next(name for name in names if name != __name__)


def resource_stream(subpath):
    module_name = external_caller()
    return files(module_name).joinpath(subpath).open()


def resource_text(subpath):
    with resource_stream(subpath) as f:
        return f.read()
