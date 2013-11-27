# caterpillar: Tests for the caterpillar.processing.schema module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import csv
import os
import pytest

from caterpillar.processing import schema
from caterpillar.processing.index import Index
from caterpillar.processing.schema import BOOLEAN, FieldType, ID, NUMERIC, Schema, TEXT, FieldConfigurationError


# Plumbing tests
def test_schema():
    simple_schema = Schema(test=TEXT, user=ID)
    names = simple_schema.names()
    items = simple_schema.items()

    schema_str = simple_schema.dumps()
    loaded_schema = Schema.loads(schema_str)
    assert len(loaded_schema.items()) == len(simple_schema.items())
    assert loaded_schema['user'].categorical()

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
    assert not 'text' in simple_schema

    with pytest.raises(FieldConfigurationError):
        simple_schema.add("_test", TEXT)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("a test", TEXT)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("test", TEXT)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("text", Index)
    with pytest.raises(FieldConfigurationError):
        simple_schema.add("text", str)

    with pytest.raises(KeyError):
        simple_schema.remove('text')
    simple_schema.remove('test')
    assert 'test' not in simple_schema

    with pytest.raises(ValueError):
        NUMERIC(num_type=str)
    with pytest.raises(NotImplementedError):
        FieldType().equals('a', 'b')
    with pytest.raises(ValueError):
        list(NUMERIC().analyse('notanumber'))

    f = NUMERIC(num_type=float)
    assert f.equals('1', '1.0')

    assert list(BOOLEAN().analyse('1'))[0].value is True


# Functional tests
def test_csv_has_header_sentiment():
    """Test function for recognising headers for small CSV file."""
    with open(os.path.abspath('caterpillar/resources/twitter_sentiment.csv'), 'rbU') as f:
        assert schema.csv_has_header(f.read(), csv.excel) is True


def test_csv_has_header_no_header():
    """Test function for recognising headers with a CSV file with no header row."""
    with open(os.path.abspath('caterpillar/resources/test_no_header.csv'), 'rbU') as f:
        assert schema.csv_has_header(f.read(), csv.excel) is False


def test_generate_schema_no_header():
    """Test generation of schema for CSV file with no header row."""
    with open(os.path.abspath('caterpillar/resources/test_no_header.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header is False
        assert len(csv_schema.columns) == 7


def test_generate_schema_small():
    """Test generation of schema for small CSV file."""
    with open(os.path.abspath('caterpillar/resources/test_small.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header is True
        assert csv_schema.dialect.delimiter == ','
        assert csv_schema.columns[3].type == csv_schema.columns[4].type == schema.ColumnDataType.TEXT
        assert len(csv_schema.columns) == 7


def test_generate_csv_schema_twitter():
    """Test generation of schema for twitter CSV file."""
    with open(os.path.abspath('caterpillar/resources/twitter_sentiment.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header is True
        assert csv_schema.dialect.delimiter == ','
        columns = csv_schema.columns
        assert columns[0].name == 'Sentiment'
        assert columns[0].type == schema.ColumnDataType.IGNORE
        assert columns[1].name == 'Text'
        assert columns[1].type == schema.ColumnDataType.TEXT
