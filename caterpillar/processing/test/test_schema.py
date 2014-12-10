# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""Tests for caterpillar.processing.schema"""
import csv
import os
import shutil
import tempfile
from caterpillar.storage.sqlite import SqliteStorage

import pytest

from caterpillar.processing import schema
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import IndexWriter, IndexReader, IndexConfig
from caterpillar.processing.schema import BOOLEAN, FieldType, ID, NUMERIC, Schema, TEXT, FieldConfigurationError
from caterpillar.searching.query.querystring import QueryStringQuery


# Plumbing tests
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

    assert list(BOOLEAN().analyse('1'))[0].value is True


def test_csv_schema():
    columns = [
        schema.ColumnSpec('text', schema.ColumnDataType.TEXT),
        schema.ColumnSpec('float', schema.ColumnDataType.FLOAT),
        schema.ColumnSpec('integer', schema.ColumnDataType.INTEGER),
        schema.ColumnSpec('string', schema.ColumnDataType.STRING)
    ]
    csv_schema = schema.CsvSchema(columns, True, csv.excel)

    index_schema = csv_schema.as_index_schema()

    assert len(index_schema) == len(columns)


# Functional tests
def test_csv_has_header_sentiment():
    """Test function for recognising headers for small CSV file."""
    with open(os.path.abspath('caterpillar/test_resources/twitter_sentiment.csv'), 'rbU') as f:
        assert schema.csv_has_header(f.read(), csv.excel) is True


def test_csv_has_header_no_header():
    """Test function for recognising headers with a CSV file with no header row."""
    with open(os.path.abspath('caterpillar/test_resources/test_no_header.csv'), 'rbU') as f:
        assert schema.csv_has_header(f.read(), csv.excel) is False


def test_generate_schema_no_header():
    """Test generation of schema for CSV file with no header row."""
    with open(os.path.abspath('caterpillar/test_resources/test_no_header.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header is False
        assert len(csv_schema.columns) == 7


def test_generate_schema_small():
    """Test generation of schema for small CSV file."""
    with open(os.path.abspath('caterpillar/test_resources/test_small.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header is True
        assert csv_schema.dialect.delimiter == ','
        assert csv_schema.columns[3].type == csv_schema.columns[4].type == schema.ColumnDataType.TEXT
        assert len(csv_schema.columns) == 7


def test_generate_csv_schema_twitter():
    """Test generation of schema for twitter CSV file."""
    with open(os.path.abspath('caterpillar/test_resources/twitter_sentiment.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header is True
        assert csv_schema.dialect.delimiter == ','
        columns = csv_schema.columns
        assert columns[0].name == 'Sentiment'
        assert columns[0].type == schema.ColumnDataType.IGNORE
        assert columns[1].name == 'Text'
        assert columns[1].type == schema.ColumnDataType.TEXT

        index_schema = csv_schema.as_index_schema(['good quality'])
        assert len(index_schema) == 1
        assert isinstance(index_schema['Text'], TEXT)

        f.seek(0)
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            assert len(csv_schema.map_row(row)) == 1


def test_index_stored_fields():
    path = tempfile.mkdtemp()
    try:
        tmp_dir = os.path.join(path, "tmp")
        analyser = DefaultAnalyser(stopword_list=stopwords.ENGLISH_TEST)
        with IndexWriter(tmp_dir, IndexConfig(SqliteStorage,
                                              Schema(text=TEXT(analyser=analyser, stored=False),
                                                     test=NUMERIC(stored=True),
                                                     test2=BOOLEAN(stored=False)))) as writer:
            doc_id = writer.add_document(text="hello world", test=777, test2=True,
                                         frame_size=2, fold_case=False, update_index=True)

        with IndexReader(tmp_dir) as reader:
            searcher = reader.searcher()
            hit = searcher.search(QueryStringQuery("*"), limit=1)[0]
            assert 'text' not in hit.data
            assert 'test2' not in hit.data
            assert hit.data['test'] == 777

            doc = reader.get_document(doc_id)
            assert 'text' not in doc
            assert 'test2' not in doc
            assert doc['test'] == 777
    finally:
        shutil.rmtree(path)
