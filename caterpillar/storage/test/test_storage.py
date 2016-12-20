# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Test for caterpillar.storage.__init__.py"""
from caterpillar import abstract_method_tester
from caterpillar.storage import StorageReader, StorageWriter


def test_storage_abc():
    """This is necessary to get 100% coverage :("""
    abstract_method_tester(StorageReader)
    abstract_method_tester(StorageWriter)
    assert True
