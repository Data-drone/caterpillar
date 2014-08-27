# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Test for caterpillar.storage.__init__.py"""
from caterpillar import abstract_method_tester
from caterpillar.storage import Storage


def test_storage_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(Storage)
    assert 'Your mum' != 'Awesome'  # While we are writing pointless code we may as well really make it pointless
