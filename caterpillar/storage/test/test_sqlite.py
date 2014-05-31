# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@kapiche.com>
import os
import shutil
import tempfile

import pytest

from caterpillar.storage import *
from caterpillar.storage.sqlite import SqliteStorage


@pytest.fixture
def tmp_dir(request):
    path = tempfile.mkdtemp()

    def clean():
        shutil.rmtree(path)

    request.addfinalizer(clean)
    new_path = os.path.join(path, "test")
    os.makedirs(new_path)
    return new_path


def test_sqlite_storage_container(tmp_dir):
    storage = SqliteStorage(tmp_dir, create=True)

    storage.begin()
    storage.add_container("test")
    storage.set_container_item("test", "A", "Z")
    storage.commit()
    assert sum(1 for _ in storage.get_container_items("test")) == storage.get_container_len("test") == 1
    assert [k for k in storage.get_container_keys("test")] == ['A']

    storage.begin()
    storage.clear_container("test")
    storage.commit()
    assert {k: v for k, v in storage.get_container_items("test")} == {}

    storage.begin()
    storage.set_container_item("test", "abc", "def")
    storage.set_container_item("test", "1", "2")
    storage.commit()
    assert sum(1 for _ in storage.get_container_items("test")) == 2
    assert storage.get_container_item("test", "abc") == "def"

    # Clear storage
    storage.begin()
    storage.clear()
    storage.commit()

    with pytest.raises(ContainerNotFoundError):
        sum(1 for _ in storage.get_container_items("test"))  # is a generator so unless we call next, no exception
    with pytest.raises(ContainerNotFoundError):
        storage.get_container_len("fake")

    storage.begin()
    with pytest.raises(ContainerNotFoundError):
        storage.clear_container("test")
    with pytest.raises(ContainerNotFoundError):
        storage.delete_container("fake")
    with pytest.raises(ContainerNotFoundError):
        sum(1 for _ in storage.get_container_keys("fake"))
    with pytest.raises(ContainerNotFoundError):
        storage.delete_container_item("fake", "bad")
    with pytest.raises(ContainerNotFoundError):
        storage.get_container_item("fake", "bad")
    with pytest.raises(ContainerNotFoundError):
        storage.set_container_item("fake", "bad", "bad")
    with pytest.raises(ContainerNotFoundError):
        sum(1 for _ in storage.set_container_items("fake", {"bad": "bad"}))
    with pytest.raises(ContainerNotFoundError):
        storage.delete_container_items("fake", ["bad"])
    storage.rollback()

    storage.begin()
    with pytest.raises(DuplicateContainerError):
        storage.add_container("test")
        storage.add_container("test")
    storage.commit()

    storage.begin()
    storage.set_container_items("test", {
        1: 'test',
        2: 'test2',
        3: 'test3'
    })
    storage.delete_container_item("test", 1)
    storage.commit()
    assert sum(1 for _ in storage.get_container_items("test")) == 2
    assert sum(1 for _ in storage.get_container_items("test", keys=('2', '3', '4', '5'))) == 4

    storage.begin()
    storage.delete_container("test")
    with pytest.raises(ContainerNotFoundError):
        sum(1 for _ in storage.get_container_items("test"))
    storage.commit()

    storage.begin()
    storage.add_container("test")
    storage.set_container_item("test", "abc", "efg")
    storage.clear_container("test")
    with pytest.raises(KeyError):
        storage.get_container_item("test", "abc")
    storage.commit()


def test_duplicate_database(tmp_dir):
    SqliteStorage(tmp_dir, create=True)
    with pytest.raises(DuplicateStorageError):
        SqliteStorage(tmp_dir, create=True)


def test_open(tmp_dir):
    storage = SqliteStorage(tmp_dir, create=True)

    storage.begin()
    storage.add_container("test")
    storage.set_container_item("test", "1", "2")
    storage.commit()
    storage.close()

    storage = SqliteStorage(tmp_dir)
    assert storage.get_container_item("test", "1") == "2"

    with pytest.raises(StorageNotFoundError):
        SqliteStorage("fake")
