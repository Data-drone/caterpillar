# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@kapiche.com>
"""Tests for caterpillar.storage.sqlite.py."""
import os
import shutil
import tempfile

import pytest
import json

from caterpillar.storage import StorageNotFoundError, DuplicateStorageError
from caterpillar.storage.sqlite import SqliteReader, SqliteWriter


@pytest.fixture
def tmp_dir(request):
    path = tempfile.mkdtemp()

    def clean():
        shutil.rmtree(path)

    request.addfinalizer(clean)
    new_path = os.path.join(path, "test")
    os.makedirs(new_path)
    return new_path


def test_add_get_delete_fields(tmp_dir):
    """ Test adding indexed fields to the schema. """
    writer = SqliteWriter(tmp_dir, create=True)

    add_fields1 = ['test', 'test2']
    add_fields2 = ['test1']
    writer.begin()
    writer.add_structured_fields(add_fields1)
    writer.add_unstructured_fields(add_fields2)
    writer.commit()

    reader = SqliteReader(tmp_dir)
    reader.begin()
    structured = reader.structured_fields
    unstructured = reader.unstructured_fields
    reader.commit()

    for field in structured:
        assert field in add_fields1
    for field in unstructured:
        assert field in add_fields2


def test_alternate_document_format(tmp_dir):
    pass


def test_add_get_document(tmp_dir):
    sample_format_document = (
        'An example document without anything fancy',
        {'test_field': 1, 'other_field': 'other'},
        {'text': ['An example', 'document without', 'anything fancy']},
        {'text': [{'An': 1, 'example': 1},
                  {'document': 1, 'without': 1},
                  {'anything': 1, 'fancy': 1}]}
    )

    writer = SqliteWriter(tmp_dir, create=True)

    # Add one document
    writer.begin()
    writer.add_structured_fields(['test_field', 'other_field'])
    writer.add_unstructured_fields(['text'])
    writer.add_analyzed_document('test', sample_format_document)
    writer.commit()

    reader_transaction = SqliteReader(tmp_dir)
    reader_transaction.begin()

    reader = SqliteReader(tmp_dir)

    doc = reader.get_document(1)  # Cheating with sequential document_id's here
    assert doc == sample_format_document[0]
    assert reader.count_documents() == 1 == reader_transaction.count_documents()
    assert reader.vocabulary_count() == 6 == reader_transaction.vocabulary_count()

    # Add 100 more documents:
    writer.begin()
    for i in range(100):
        writer.add_analyzed_document('test', sample_format_document)
    writer.commit()
    assert reader.count_documents() * 3 == 303 == reader.count_frames()
    assert reader_transaction.count_documents() == 1
    assert reader.vocabulary_count() == 6
    assert sum(i[1] for i in reader.get_frequencies()) == 606

    reader_transaction.commit()
    assert reader_transaction.count_documents() == 101

    meta = list(reader.get_metadata())
    assert len(meta) == 2

    # Delete all the documents
    writer.begin()
    writer.delete_documents([d_id for d_id, _ in reader.iterate_documents()])
    writer.commit()

    assert reader.count_documents() == 0 == reader.count_frames()
    assert reader.vocabulary_count() == 6
    assert sum(i[1] for i in reader.get_frequencies()) == 0


def test_a(tmp_dir):
    pass


def test_(tmp_dir):
    pass


def old():
    storage.add_container("test")
    storage.set_container_item("test", "A", "Z")
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
    # Test that an empty set of keys returns no rows
    assert sum(1 for _ in storage.get_container_items("test", keys=[])) == 0

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
    SqliteWriter(tmp_dir, create=True)
    with pytest.raises(DuplicateStorageError):
        SqliteWriter(tmp_dir, create=True)


def open(tmp_dir):
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
