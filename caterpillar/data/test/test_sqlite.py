# caterpillar - tests for caterpillar.data.sqlite module.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import os

import pytest

from caterpillar.data.storage import *
from caterpillar.data.sqlite import SqliteStorage, SqliteMemoryStorage


DB = 'test-storage.db'


@pytest.fixture(scope="function", autouse=True)
def delete_databases():
    if os.path.isfile(DB):
        os.remove(DB)


def test_sqlite_storage():

    storage = None
    try:
        storage = SqliteStorage.create(DB, os.getcwd())

        storage.add_container("test")
        storage.set_container_item("test", "A", "Z")
        assert len(storage.get_container_items("test")) == 1
        storage.clear(container="test")
        assert storage.get_container_items("test") == {}

        storage.set_container_item("test", "abc", "def")
        storage.set_container_item("test", "1", "2")
        assert len(storage.get_container_items("test")) == 2
        assert storage.get_container_item("test", "abc") == "def"

        storage.clear()

        with pytest.raises(ContainerNotFoundError):
            storage.get_container_items("test")
        with pytest.raises(ContainerNotFoundError):
            storage.clear_container("test")

        with pytest.raises(ContainerNotFoundError):
            storage.delete_container("fake")
        with pytest.raises(ContainerNotFoundError):
            storage.delete_container_item("fake", "bad")
        with pytest.raises(ContainerNotFoundError):
            storage.delete_container_items("fake", ["bad"])
        with pytest.raises(ContainerNotFoundError):
            storage.get_container_item("fake", "bad")
        with pytest.raises(ContainerNotFoundError):
            storage.set_container_item("fake", "bad", "bad")
        with pytest.raises(ContainerNotFoundError):
            storage.set_container_items("fake", {"bad": "bad"})

        with pytest.raises(DuplicateContainerError):
            storage.add_container("test")
            storage.add_container("test")

        storage.set_container_items("test", {
            1: 'test',
            2: 'test2',
            3: 'test3'
        })
        storage.delete_container_item("test", 1)
        assert len(storage.get_container_items("test")) == 2
        assert len(storage.get_container_items("test", keys=(2, 3, 4, 5))) == 4

        with pytest.raises(ContainerNotFoundError):
            storage.delete_container("test")
            storage.get_container_items("test")

        with pytest.raises(KeyError):
            storage.add_container("test")
            storage.set_container_item("test", "abc", "efg")
            storage.clear_container("test")
            storage.get_container_item("test", "abc")

    finally:
        if storage:
            path = storage.get_db_path()
            storage.destroy()
            assert not os.path.isfile(path)


def test_duplicate_database():
    storage = SqliteStorage.create(DB, os.getcwd())
    with pytest.raises(DuplicateStorageError):
        storage = SqliteStorage.create(DB, os.getcwd())


def test_open():
    storage = SqliteStorage.create(DB, os.getcwd(), acid=False, containers=("test",))
    storage.set_container_item("test", "1", "2")
    storage.close()
    storage = SqliteStorage.open(DB, os.getcwd())
    assert storage.get_container_item("test", "1") == "2"

    with pytest.raises(StorageNotFoundError):
        SqliteStorage.open("fake", "fake")


def test_memory_storage():

    with pytest.raises(NotImplementedError):
            SqliteMemoryStorage.open('test')

    storage = None
    try:
        storage = SqliteMemoryStorage.create(DB)

        storage.add_container("test")
        storage.set_container_item("test", "A", "Z")
        assert len(storage.get_container_items("test")) == 1
        storage.clear(container="test")
        assert storage.get_container_items("test") == {}

        storage.set_container_item("test", "abc", "def")
        storage.set_container_item("test", "1", "2")
        assert len(storage.get_container_items("test")) == 2
        assert storage.get_container_item("test", "abc") == "def"

        storage.clear()

        with pytest.raises(ContainerNotFoundError):
            storage.get_container_items("test")
        with pytest.raises(ContainerNotFoundError):
            storage.clear_container("test")

        with pytest.raises(ContainerNotFoundError):
            storage.delete_container("fake")
        with pytest.raises(ContainerNotFoundError):
            storage.delete_container_item("fake", "bad")
        with pytest.raises(ContainerNotFoundError):
            storage.get_container_item("fake", "bad")
        with pytest.raises(ContainerNotFoundError):
            storage.set_container_item("fake", "bad", "bad")
        with pytest.raises(ContainerNotFoundError):
            storage.set_container_items("fake", {"bad": "bad"})

        with pytest.raises(DuplicateContainerError):
            storage.add_container("test")
            storage.add_container("test")

        storage.set_container_items("test", {
            1: 'test',
            2: 'test2',
            3: 'test3'
        })
        storage.delete_container_item("test", 1)
        assert len(storage.get_container_items("test")) == 2
        assert len(storage.get_container_items("test", keys=(2, 3, 4, 5))) == 4

        with pytest.raises(ContainerNotFoundError):
            storage.delete_container("test")
            storage.get_container_items("test")

        with pytest.raises(KeyError):
            storage.add_container("test")
            storage.set_container_item("test", "abc", "efg")
            storage.clear_container("test")
            storage.get_container_item("test", "abc")

    finally:
        if storage:
            storage.destroy()
