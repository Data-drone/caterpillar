# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@kapiche.com>
"""Tests for caterpillar.storage.sqlite.py."""

import os
import pytest
import shutil

import apsw

from caterpillar.storage import StorageNotFoundError, DuplicateStorageError
from caterpillar.storage.sqlite import (
    SqliteReader, SqliteWriter, _count_bitwise_matches, SqliteSchemaMismatchError, CURRENT_SCHEMA, MigrationError
)


def test_add_get_delete_fields(index_dir):
    """ Test adding indexed fields to the schema. """
    writer = SqliteWriter(index_dir, create=True)

    add_fields1 = {'test': dict(type='text', arbitrary=7), 'test2': dict(type='text')}
    add_fields2 = {'test1': {'potato': 'vegetable'}, '': {}}
    writer.begin()
    writer.add_fields(**add_fields1)
    writer.add_fields(**add_fields2)
    writer.add_fields(testing={})
    writer.commit()

    reader = SqliteReader(index_dir)
    reader.begin()
    fields = reader.fields
    reader.commit()

    for field in fields:
        assert (
            field in add_fields1 or
            field in add_fields2 or
            field == 'testing'
        )


def test_nonexistent_path(index_dir):
    with pytest.raises(StorageNotFoundError):
        SqliteWriter(index_dir + '/nonexistent_dir')


def test_bad_document_format(index_dir):
    writer = SqliteWriter(index_dir, create=True)

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
    writer.add_fields(text=dict(type='text', field_setting=42))

    # Non matching fields
    with pytest.raises(ValueError):
        writer.add_analyzed_document('v1', bad_document)

    # Non matching numbers of frames and positions:
    bad_document[2] = {'text': ['An example', 'frame']}
    with pytest.raises(ValueError):
        writer.add_analyzed_document('v1', bad_document)

    with pytest.raises(ValueError):
        writer.add_analyzed_document('unknown_format', bad_document)

    writer.commit()
    writer.close()


def test_add_get_document(index_dir):

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

    writer = SqliteWriter(index_dir, create=True)

    # Add one document
    writer.begin()
    writer.add_fields(
        test_field=dict(a=1, b=2, c=3),
        other_field=dict(type='salad'),
        text={}
    )

    writer.add_analyzed_document('v1', sample_format_document)

    with pytest.raises(apsw.SQLError):
        writer._execute('select * from nonexistent_table')
    with pytest.raises(apsw.SQLError):
        writer._executemany('insert into nonexistent_table values(?)', [(None,)])

    writer.commit()

    reader_transaction = SqliteReader(index_dir)
    reader_transaction.begin()

    reader = SqliteReader(index_dir)

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


def test_iterators(index_dir):

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

    writer = SqliteWriter(index_dir, create=True)

    # Add many documents.
    writer.begin()
    writer.add_fields(
        test_field=dict(a=1, b=2, c=3),
        other_field=dict(type='salad'),
        text={}
    )
    for i in range(100):
        writer.add_analyzed_document('v1', sample_format_document)
    writer.commit()

    assert len(writer._SqliteWriter__last_added_documents) == 100

    reader = SqliteReader(index_dir)
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


def test_filter_error(index_dir):

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

    writer = SqliteWriter(index_dir, create=True)

    # Add many documents.
    writer.begin()
    writer.add_fields(
        test_field=dict(a=1, b=2, c=3),
        other_field=dict(type='salad'),
        text={}
    )
    for i in range(100):
        writer.add_analyzed_document('v1', sample_format_document)
    writer.commit()

    reader = SqliteReader(index_dir)
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
    shutil.copytree('caterpillar/test_resources/alice_indexed_0_10_0', migrate_index)
    writer = SqliteWriter(migrate_index)
    reader = SqliteReader(migrate_index)
    writer2 = SqliteWriter(migrate_index)
    cursor = writer._db_connection.cursor()

    with pytest.raises(SqliteSchemaMismatchError):
        writer.begin()

    with pytest.raises(SqliteSchemaMismatchError):
        reader.begin()

    schema_version = writer.migrate()
    assert schema_version == CURRENT_SCHEMA
    # Ensure migrations are idempotent
    schema_version = writer.migrate()
    assert schema_version == CURRENT_SCHEMA

    writer.begin()
    with pytest.raises(Exception):
        writer2.begin(timeout=1)
    writer.commit()

    reader.begin()
    reader.commit()

    # Mess with the schema version table, just to simulate newer schema versions than current.
    cursor.execute('insert into migrations(id) values (?)', [schema_version + 1])
    with pytest.raises(Exception):
        reader.begin()

    with pytest.raises(Exception):
        writer.begin()

    cursor.execute('delete from migrations where id = ?', [schema_version + 1])

    with pytest.raises(ValueError):
        writer.migrate(version=1000)

    with pytest.raises(ValueError):
        writer.migrate(version=-1)

    writer.begin()
    with pytest.raises(Exception):
        writer2.migrate(timeout=1)
    writer.rollback()

    schema_version = writer.migrate(0)
    assert schema_version == 0

    with pytest.raises(ValueError):
        writer.migrate(version=0.5)
    assert writer.schema_version == 0

    # Mangle the on disk schema so that a migration fails and rollsback correctly
    cursor.execute('create table field_setting (test integer primary key)')
    with pytest.raises(MigrationError):
        writer.migrate(version=1)

    assert writer.schema_version == 0


def test_duplicate_database(index_dir):
    SqliteWriter(index_dir, create=True)
    with pytest.raises(DuplicateStorageError):
        SqliteWriter(index_dir, create=True)


def test_negative_positions():
    assert _count_bitwise_matches(-1) == 0
