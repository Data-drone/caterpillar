# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>, Kris Rogers <kris@kapiche.com>
"""Tests for plugin module."""
import os

from caterpillar import abstract_method_tester
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import IndexConfig, IndexWriter, IndexReader
from caterpillar.processing.plugin import AnalyticsPlugin
from caterpillar.processing import schema
from caterpillar.storage.sqlite import SqliteStorage


class TrivialTestPlugin(AnalyticsPlugin):
    """
    A very simple plugin for testing.

    """
    @staticmethod
    def get_name():
        return 'trivial_test_plugin'

    def run(self):
        return {
            'fake_data': {'a': 100}
        }


def test_plugin(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        analyser = DefaultAnalyser(stopword_list=stopwords.ENGLISH_TEST)
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, schema.Schema(text=schema.TEXT(analyser=analyser)))) as \
                writer:
            writer.add_document(text=data, frame_size=2)
            writer.run_plugin(TrivialTestPlugin)
        with IndexReader(index_dir) as reader:
            assert int(dict(reader.get_plugin_data(TrivialTestPlugin, 'fake_data'))['a']) == 100
        with IndexWriter(index_dir) as writer:
            writer.run_plugin(TrivialTestPlugin)  # Overwrite plugin results


def test_plugin_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(AnalyticsPlugin)
    assert 'Your mum' != 'Awesome'  # While we are writing pointless code we may as well really make it pointless
