# caterpillar: Tests for the caterpillar.processing.schema module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import csv
import os

from caterpillar.processing import schema


def test_csv_has_header_sentiment():
    """Test function for recognising headers for small CSV file."""
    with open(os.path.abspath('caterpillar/resources/twitter_sentiment.csv'), 'rbU') as f:
        assert schema.csv_has_header(f.read(), csv.excel) == True


def test_csv_has_header_no_header():
    """Test function for recognising headers with a CSV file with no header row."""
    with open(os.path.abspath('caterpillar/resources/test_no_header.csv'), 'rbU') as f:
        assert schema.csv_has_header(f.read(), csv.excel) == False


def test_generate_schema_no_header():
    """Test generation of schema for CSV file with no header row."""
    with open(os.path.abspath('caterpillar/resources/test_no_header.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header == False
        assert len(csv_schema.columns) == 7


def test_generate_schema_small():
    """Test generation of schema for small CSV file."""
    with open(os.path.abspath('caterpillar/resources/test_small.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header == True
        assert csv_schema.dialect.delimiter == ','
        assert csv_schema.columns[3].type ==  csv_schema.columns[4].type ==  schema.ColumnDataType.TEXT
        assert len(csv_schema.columns) == 7


def test_generate_csv_schema_twitter():
    """Test generation of schema for twitter CSV file."""
    with open(os.path.abspath('caterpillar/resources/twitter_sentiment.csv'), 'rbU') as f:
        csv_schema = schema.generate_csv_schema(f)
        assert csv_schema.has_header == True
        assert csv_schema.dialect.delimiter == ','
        columns = csv_schema.columns
        assert columns[0].name == 'Sentiment'
        assert columns[0].type == schema.ColumnDataType.IGNORE
        assert columns[1].name == 'Text'
        assert columns[1].type == schema.ColumnDataType.TEXT
