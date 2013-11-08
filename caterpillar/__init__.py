def abstract_method_tester(abc):
    """Utility function to test abstract classes"""

    def init(self):
        pass

    def tester(self, abc_method_name):
        abc_method = abc.__dict__[abc_method_name]
        args = (None,) * (abc_method.func_code.co_argcount-1)
        abc_method(self, *args)

    methods = {'__init__': init}
    methods.update((n, (lambda n: lambda self: tester(self, n))(n)) for n in abc.__abstractmethods__)
    subinstance = type(abc.__name__ + "_test", (abc,), methods)()
    for method_name in sorted(abc.__abstractmethods__):
        methods[method_name](subinstance)


def get_full_cls_name(obj):
    """
    Get fully qualified class name for the specified object.

    """
    return obj.__module__ + '.' + obj.__class__.__name__


def get_cls(cls_name):
    """
    Get class reference from fully qualified name.

    """
    parts = cls_name.split('.')
    module = '.'.join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m
