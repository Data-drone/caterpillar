# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@kapiche.com>
"""Tests for caterpillar.storage.sqlite.py."""
import os
import shutil
import tempfile

import pytest
import apsw

from caterpillar.storage import StorageNotFoundError, DuplicateStorageError
from caterpillar.storage.sqlite import (
    SqliteReader, SqliteWriter, _count_bitwise_matches, CURRENT_SCHEMA,
    MigrationError, SqliteSchemaMismatchError
)


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
    add_fields2 = ['test1', '']
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


def test_nonexistent_path(tmp_dir):
    with pytest.raises(StorageNotFoundError):
        SqliteWriter(tmp_dir + '/nonexistent_dir')


def test_alternate_document_format(tmp_dir):
    pass


def test_bad_document_format(tmp_dir):
    writer = SqliteWriter(tmp_dir, create=True)

    bad_document = [
        'A badly formatted document',
        {},
        {'text': ['An example', 'document without', 'anything fancy'],
         'invalid_field': []},
        {'text': [
            {'An': [0, 5, 7], 'example': [0, 5, 7]},
            {'document': [0, 5, 7], 'without': [0, 5, 7]},
            {'anything': [0, 5, 7], 'fancy': [0, 5, 7]}
        ]}
    ]

    writer.begin()
    writer.add_unstructured_fields(['text'])

    # Non matching fields
    with pytest.raises(ValueError):
        writer.add_analyzed_document('v1', bad_document)

    # Non matching numbers of frames and positions:
    bad_document[2] = {'text': ['An example', 'frame']}
    with pytest.raises(ValueError):
        writer.add_analyzed_document('v1', bad_document)

    with pytest.raises(ValueError):
        writer.add_analyzed_document('unknown_format', bad_document)

    writer.close()


def test_add_get_document(tmp_dir):

    sample_format_document = (
        'An example document without anything fancy',
        {'test_field': 1, 'other_field': 'other'},
        {'text': ['An example', 'document without', 'anything fancy']},
        {'text': [
            {'An': [0, 5, 7], 'example': [0, 5, 7]},
            {'document': [0, 5, 7], 'without': [0, 5, 7]},
            {'anything': [0, 5, 7], 'fancy': [0, 5, 7]}
        ]}
    )

    writer = SqliteWriter(tmp_dir, create=True)

    # Add one document
    writer.begin()
    writer.add_structured_fields(['test_field', 'other_field'])
    writer.add_unstructured_fields(['text'])
    writer.add_analyzed_document('v1', sample_format_document)

    with pytest.raises(apsw.SQLError):
        writer._execute('select * from nonexistent_table')
    with pytest.raises(apsw.SQLError):
        writer._executemany('insert into nonexistent_table values(?)', [(None,)])

    writer.commit()

    reader_transaction = SqliteReader(tmp_dir)
    reader_transaction.begin()

    reader = SqliteReader(tmp_dir)

    with pytest.raises(apsw.SQLError):
        reader._execute('select * from nonexistent_table')
    with pytest.raises(apsw.SQLError):
        reader._executemany('insert into nonexistent_table values(?)', [(None,)])

    doc = list(reader.iterate_documents([1]))[0]  # Cheating with sequential document_id's here
    assert doc[1] == sample_format_document[0]
    assert reader.count_documents() == 1 == reader_transaction.count_documents()
    assert reader.count_vocabulary() == 6 == reader_transaction.count_vocabulary()

    # Add 100 more documents:
    writer.begin()
    for i in range(100):
        writer.add_analyzed_document('v1', sample_format_document)
    writer.commit()

    assert reader.count_documents() * 3 == 303 == reader.count_frames()
    assert reader_transaction.count_documents() == 1
    assert reader.count_vocabulary() == 6
    assert sum(i[1] for i in reader.iterate_term_frequencies()) == 606

    reader_transaction.commit()
    assert reader_transaction.count_documents() == 101

    meta = list(reader.iterate_metadata())
    assert len(meta) == 2

    # Term associations
    associations = {term: values for term, values in reader.iterate_associations()}
    assert len(associations) == 6
    assert all([freq == 101 for values in associations.values() for freq in values.values()])

    # Delete all the documents
    writer.begin()
    writer.delete_documents([d_id for d_id, _ in reader.iterate_documents()])
    writer.commit()

    assert reader.count_documents() == 0 == reader.count_frames()
    assert reader.count_vocabulary() == 6
    assert sum(i[1] for i in reader.iterate_term_frequencies()) == 0


def test_iterators(tmp_dir):

    sample_format_document = (
        'An example document without anything fancy',
        {'test_field': 1, 'other_field': 'other'},
        {'text': ['An example', 'document without', 'anything fancy']},
        {'text': [
            {'An': [0, 5, 7], 'example': [0, 5, 7]},
            {'document': [0, 5, 7], 'without': [0, 5, 7]},
            {'anything': [0, 5, 7], 'fancy': [0, 5, 7]}
        ]}
    )

    writer = SqliteWriter(tmp_dir, create=True)

    # Add many documents.
    writer.begin()
    writer.add_structured_fields(['test_field', 'other_field'])
    writer.add_unstructured_fields(['text'])
    for i in range(100):
        writer.add_analyzed_document('v1', sample_format_document)
    writer.commit()

    assert len(writer._SqliteWriter__last_added_documents) == 100

    reader = SqliteReader(tmp_dir)
    reader.begin()

    positions = reader._iterate_positions(include_fields=['text'])
    assert sum(1 for _ in positions) == 6
    positions = reader._iterate_positions(exclude_fields=['text'])
    assert sum(1 for _ in positions) == 0
    positions = reader._iterate_positions()
    assert sum(1 for _ in positions) == 6

    positions = reader._iterate_positions(include_fields=['unknown field'])
    # If the field is not indexed, raise an error
    with pytest.raises(ValueError):
        list(reader._iterate_positions(include_fields=['unknown field']))

    metadata_frames = [
        (field, values, documents) for field, values, documents in reader.iterate_metadata(frames=True)
    ]
    assert sum(1 for _ in metadata_frames) == 2
    assert sum(len(i[2]) for i in metadata_frames) == 600

    # Get documents corresponding to the metadata instead of just the frames.
    metadata_documents = [
        (field, values, documents) for field, values, documents in reader.iterate_metadata(frames=False)
    ]
    assert sum(1 for _ in metadata_documents) == 2
    assert sum(len(i[2]) for i in metadata_documents) == 200

    metadata_text = [
        (field, values, documents) for field, values, documents
        in reader.iterate_metadata(text_field='text', include_fields=['test_field'])
    ]
    assert sum(1 for _ in metadata_text) == 1
    assert sum(len(i[2]) for i in metadata_text) == 300

    metadata_field = [
        (field, values, documents) for field, values, documents
        in reader.iterate_metadata(include_fields=['test_field'])
    ]
    assert sum(1 for _ in metadata_field) == 1
    assert sum(len(i[2]) for i in metadata_field) == 300

    metadata_no_field = [
        (field, values, documents) for field, values, documents
        in reader.iterate_metadata(text_field='text')
    ]
    assert sum(1 for _ in metadata_no_field) == 2
    assert sum(len(i[2]) for i in metadata_no_field) == 600

    associations = [
        row for row in reader.iterate_associations(include_fields=['text'])
    ]

    assert len(associations) == 6
    assert all([len(other) == 1 for _, other in associations])

    reader.close()


def test_filter_error(tmp_dir):

    sample_format_document = (
        'An example document without anything fancy',
        {'test_field': 1, 'other_field': 'other'},
        {'text': ['An example', 'document without', 'anything fancy']},
        {'text': [
            {'An': [0, 5, 7], 'example': [0, 5, 7]},
            {'document': [0, 5, 7], 'without': [0, 5, 7]},
            {'anything': [0, 5, 7], 'fancy': [0, 5, 7]}
        ]}
    )

    writer = SqliteWriter(tmp_dir, create=True)

    # Add many documents.
    writer.begin()
    writer.add_structured_fields(['test_field', 'other_field'])
    writer.add_unstructured_fields(['text'])
    for i in range(100):
        writer.add_analyzed_document('v1', sample_format_document)
    writer.commit()

    reader = SqliteReader(tmp_dir)
    reader.begin()

    with pytest.raises(ValueError):
        reader.rank_or_filter_unstructured(must=['example'], metadata={'test_field': {'*=': 1}})
    with pytest.raises(ValueError):
        reader.rank_or_filter_unstructured(metadata={'test_field': {'*=': 1}}, search=True)
    with pytest.raises(ValueError):
        reader.filter_metadata(metadata={'test_field': {'*=': 1}})

    reader.close()


def test_open_migrate_old_schema_version(index_dir):
    """Open and attempt to operate storage created with an earlier version. """
    # Copy the old index to the temp dir, as we need to modify it.
    migrate_index = os.path.join(index_dir, 'sample_index')
    shutil.copytree('caterpillar/test_resources/alice_indexed_v0_10_0', migrate_index)
    writer = SqliteWriter(migrate_index)
    reader = SqliteReader(migrate_index)
    cursor = writer._db_connection.cursor()

    # Mismatched versions raise errors on begin
    with pytest.raises(SqliteSchemaMismatchError):
        writer.begin()

    with pytest.raises(SqliteSchemaMismatchError):
        reader.begin()

    # Test migrating to a newer version that doesn't exist
    # This relies on the on disk version being None, and -1 > None in Python
    with pytest.raises(MigrationError):
        writer.migrate(version=-1)

    # Failed migrations should rollback - test by premangling the schema.
    cursor.execute('create table migrations (test integer primary key)')
    with pytest.raises(MigrationError):
        writer.migrate()
    assert writer.schema_version is None
    cursor.execute('drop table migrations')

    schema_version = writer.migrate()
    assert schema_version == CURRENT_SCHEMA
    # Ensure migrations are idempotent
    schema_version = writer.migrate()
    assert schema_version == CURRENT_SCHEMA

    # Now that the database is at a real numerical version,
    # test migrating to an older version that doesn't exist
    with pytest.raises(MigrationError):
        writer.migrate(version=-1)

    # Mess with the schema version table, just to simulate newer schema versions than current.
    cursor.execute('insert into migrations(id) values (?)', [schema_version + 1])
    with pytest.raises(Exception):
        reader.begin()

    with pytest.raises(Exception):
        writer.begin()

    cursor.execute('delete from migrations where id = ?', [schema_version + 1])

    schema_version = writer.migrate(0)
    assert schema_version == 0

    with pytest.raises(MigrationError):
        writer.migrate(version=CURRENT_SCHEMA + 1)


def test_duplicate_database(tmp_dir):
    SqliteWriter(tmp_dir, create=True)
    with pytest.raises(DuplicateStorageError):
        SqliteWriter(tmp_dir, create=True)


def test_negative_positions():
    assert _count_bitwise_matches(-1) == 0
