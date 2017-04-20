# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""Tests for caterpillar.processing.schema"""
from caterpillar.storage.sqlite import SqliteReader, SqliteWriter

import pytest

from caterpillar.processing.analysis.analyse import DateTimeAnalyser
from caterpillar.processing.index import IndexWriter, IndexReader
from caterpillar.processing.schema import (
    BOOLEAN, FieldType, ID, NUMERIC, Schema, TEXT, FieldConfigurationError, DATETIME, CATEGORICAL_TEXT
)


def test_schema():
    simple_schema = Schema(test=TEXT, user=ID)
    names = simple_schema.names()
    items = simple_schema.items()

    assert len(simple_schema) == 2
    assert len(names) == 2
    assert 'test' in names
    assert 'user' in names
    assert len(items) == 2

    assert isinstance(simple_schema['test'], TEXT)
    assert isinstance(simple_schema['user'], ID)
    with pytest.raises(KeyError):
        simple_schema['no_item']

    for field in simple_schema:
        assert isinstance(field, FieldType)

    assert 'test' in simple_schema
    assert 'text' not in simple_schema

    with pytest.raises(FieldConfigurationError):
        simple_schema.add("_test", TEXT)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("test", TEXT)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("text", object)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("text", str)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("text", IndexWriter)

    with pytest.raises(ValueError):
        NUMERIC(num_type=str)
    with pytest.raises(NotImplementedError):
        FieldType().equals('a', 'b')
    with pytest.raises(ValueError):
        list(NUMERIC().analyse('notanumber'))

    f = NUMERIC(num_type=float)
    assert f.equals('1', '1.0')

    dt = DATETIME(analyser=DateTimeAnalyser(datetime_formats=['HH:mm DD/MM/YYYY']))
    assert dt.value_of('10:05 01/12/2016') == '2016-12-01T10:05:00z'
    assert dt.equals('10:05 01/12/2016', '10:05 01/12/2016')
    assert dt.gt('10:06 01/12/2016', '10:05 01/12/2016')
    assert dt.gte('10:05 01/12/2016', '10:05 01/12/2016')
    assert dt.gte('10:05 02/12/2016', '10:05 01/12/2016')
    assert dt.lt('01:05 01/12/2016', '10:05 01/12/2016')
    assert dt.lte('10:05 01/12/2016', '10:05 01/12/2016')
    assert dt.lte('10:05 01/12/2015', '10:05 01/12/2016')

    assert list(BOOLEAN().analyse('1'))[0].value is True

    c = CATEGORICAL_TEXT()
    assert c.equals('cat', 'cat')
    assert c.equals_wildcard('cat', 'ca*')


def test_index_stored_fields(index_dir):

    with IndexWriter(SqliteWriter(index_dir, create=True)) as writer:
        writer.add_fields(
            text=dict(type='test_text', stored=False),
            test=dict(type='numeric', stored=True),
            test2=dict(type='boolean', stored=False)
        )
        writer.add_document(dict(text="hello world", test=777, test2=True), frame_size=2)

    doc_id = writer.last_committed_documents[0]

    with IndexReader(SqliteReader(index_dir)) as reader:
        _, frame = list(reader.get_frames(None, frame_ids=[1]))[0]
        assert frame['_field'] not in frame
        assert 'test2' not in frame
        assert frame['test'] == 777

        doc = reader.get_document(doc_id)
        assert 'text' not in doc
        assert 'test2' not in doc
        assert doc['test'] == 777
