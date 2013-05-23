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
    'nltk',        
    'regex',        
]

setup(
    name='caterpillar',
    version='0.1',
    packages=['caterpillar', 'caterpillar.analytics', 'caterpillar.processing'],
    url='http://www.mammothlabs.com.au',
    license='Commercial',
    install_requires=requires,
    tests_require=['tox'],
    cmdclass={'test': Tox},
    author='Mammoth Labs',
    author_email='contact@mammothlabs.com.au',
    description='Mammoth Labs text analytics engine.'
)
