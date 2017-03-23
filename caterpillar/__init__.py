# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
from abc import abstractproperty
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass


def abstract_method_tester(abc):
    """Utility function to test abstract classes"""

    def tester(self, abc_method_name):
        abc_method = abc.__dict__[abc_method_name]
        if isinstance(abc_method, abstractproperty):
            abstractproperty(self)
        else:
            args = (None,) * (abc_method.func_code.co_argcount - 1)
            abc_method(self, *args)

    methods = {}
    methods.update((
        n, (lambda n: lambda self: tester(self, n))(n))
        for n in ['__init__'] + list(abc.__abstractmethods__)
    )
    subinstance = type(abc.__name__ + "_test", (abc,), methods)()
    for method in methods.itervalues():
        method(subinstance)
