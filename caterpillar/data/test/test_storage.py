# caterpillar
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import pytest
from caterpillar import abstract_method_tester
from caterpillar.data.storage import Storage, RamStorage, DuplicateContainerError, ContainerNotFoundError, DocumentNotFoundError


def test_storage_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(Storage)
    assert 'Your mum' != 'Awesome'  # While we are writing pointless code we may as well really make it pointless


def test_ram_storage():
    storage = RamStorage.create(None)
    assert storage.get_schema() is None

    storage.add_container("test")
    assert storage.get_container_items("test") == {}

    storage.set_container_item("test", "abc", "def")
    assert storage.get_container_item("test", "abc") == "def"

    with pytest.raises(KeyError):
        storage.clear()
        storage.get_container_items("test")

    with pytest.raises(DuplicateContainerError):
        storage.add_container("test")
        storage.add_container("test")

    with pytest.raises(ContainerNotFoundError):
        storage.delete_container("fake")

    with pytest.raises(KeyError):
        storage.delete_container("test")
        storage.get_container_items("test")

    with pytest.raises(KeyError):
        storage.add_container("test")
        storage.set_container_item("test", "abc", "efg")
        storage.clear_container("test")
        storage.get_container_item("test", "abc")

    storage.store_document("test", {'some': 'data'})
    assert storage.get_document("test") == {'some': 'data'}

    with pytest.raises(DocumentNotFoundError):
        storage.remove_document("test")
        storage.get_document("test")

    with pytest.raises(KeyError):
        storage.destroy()
        storage.get_container_item("test", "abc")

