# Copyright (C) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
from distutils.core import setup
import os
from setuptools.command.test import test as TestCommand
import sys


class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(self.test_args)
        sys.exit(errno)


# Use the VERSION file to get caterpillar version
version_file = os.path.join(os.path.dirname(__file__), 'caterpillar', 'VERSION')
with open(version_file) as fh:
    caterpillar_version = fh.read().strip()

requires = [
    'apsw',
    'lrparsing',
    'nltk',
    'numpy',
    'regex',
    'ujson',
]

setup(
    name='caterpillar',
    version=caterpillar_version,
    packages=[
        'caterpillar',
        'caterpillar.processing',
        'caterpillar.processing.analysis',
        'caterpillar.resources',
        'caterpillar.searching',
        'caterpillar.searching.query',
        'caterpillar.storage',
    ],
    package_data={
        'caterpillar': ['resources/*.txt'],
    },
    url='http://www.kapiche.com',
    license='Commercial',
    install_requires=requires,
    tests_require=['tox', 'pytest', 'coverage', 'pep8'],
    cmdclass={'test': Tox},
    author='Ryan Stuart',
    author_email='contact@kapiche.com',
    description='Text retrieval and analytics engine.'
)
