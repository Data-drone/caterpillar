from distutils.core import setup
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


requires = [
    'ujson',
    'nltk',
    'numpy',
    'regex',
]

setup(
    name='caterpillar',
    version='1.0-alpha',
    packages=[
        'caterpillar',
        'caterpillar.analytics',
        'caterpillar.data',
        'caterpillar.processing',
    ],
    url='http://www.mammothlabs.com.au',
    license='Commercial',
    install_requires=requires,
    tests_require=['tox', 'pytest'],
    cmdclass={'test': Tox},
    author='Mammoth Labs',
    author_email='contact@mammothlabs.com.au',
    description='Mammoth Labs text analytics engine.'
)
