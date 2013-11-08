# caterpillar - tests for caterpillar.data.storage module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar import abstract_method_tester
from caterpillar.data.storage import Storage


def test_storage_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(Storage)
    assert 'Your mum' != 'Awesome'  # While we are writing pointless code we may as well really make it pointless
