# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import os

version_file = open(os.path.join(os.path.dirname(__file__), 'VERSION'), 'r')
full_version = version_file.read().strip().split('-')
version_file.close()

VERSION = full_version[0]  # Major version number, X.Y
RELEASE = full_version[1] if len(full_version) > 1 else ''  # Release name


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
