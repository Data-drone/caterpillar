# Copyright (C) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
from distutils.core import setup
from io import open
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

# Use README.md as long_description
with open('README.rst', encoding='utf-8') as f:
    long_description = f.read()

# Use the VERSION file to get caterpillar version
version_file = os.path.join(os.path.dirname(__file__), 'caterpillar', 'VERSION')
with open(version_file, encoding='utf-8') as fh:
    caterpillar_version = fh.read().strip()

requires = [
    'apsw',
    'arrow',
    'lrparsing',
    'nltk>=2.0,<2.1',
    'numpy',
    'regex',
    'ujson',
]

setup(
    name='caterpillar',
    version=caterpillar_version,
    long_description=long_description,
    packages=[
        'caterpillar',
        'caterpillar.processing',
        'caterpillar.processing.analysis',
        'caterpillar.resources',
        'caterpillar.storage',
    ],
    package_data={
        'caterpillar': ['resources/*.txt', 'VERSION'],
    },
    url='https://github.com/Kapiche/caterpillar',
    license='AGPLv3+',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Text Processing :: Indexing',
    ],
    keywords='indexing text analytics',
    install_requires=requires,
    extras_require={
        'test': ['tox', 'pytest', 'pytest-cov', 'coverage', 'pep8', 'mock'],
    },
    cmdclass={'test': Tox},
    author='Kapiche',
    author_email='contact@kapiche.com',
    description='Text retrieval and analytics engine.'
)
