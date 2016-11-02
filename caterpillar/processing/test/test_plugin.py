# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>, Kris Rogers <kris@kapiche.com>
"""Tests for plugin module."""
import os
import ujson as json
import pytest

from caterpillar import abstract_method_tester
from caterpillar.processing.index import IndexConfig, IndexWriter, IndexReader
from caterpillar.processing.plugin import AnalyticsPlugin
from caterpillar.processing import schema
from caterpillar.storage.sqlite import SqliteStorage
from caterpillar.storage import PluginNotFoundError
from caterpillar.test_util import TestAnalyser


class TestPlugin(AnalyticsPlugin):
    """
    A very simple plugin for testing.

    """
    def __init__(self, indexreader, arbitrary_integer):
        self.index_reader = indexreader
        self.arbitrary_integer = arbitrary_integer

    @staticmethod
    def get_name():
        return 'trivial_test_plugin'

    def run(self):
        self.internal_state = [i**2 for i in range(self.arbitrary_integer)]

    def get_state(self):
        state = dict(internal_state=json.dumps(self.internal_state))
        return state

    def get_settings(self):
        return json.dumps(self.arbitrary_integer)

    def restore_from_state(self, state):
        self.internal_state = json.loads(state['internal_state'])
        return


def test_plugin(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        analyser = TestAnalyser()

        with IndexWriter(index_dir, IndexConfig(SqliteStorage, schema.Schema(text=schema.TEXT(analyser=analyser)))) as \
                writer:
            writer.add_document(text=data, frame_size=2)

        # Run some plugins, and save in the index.
        with IndexReader(index_dir) as reader:
            test_plugins = [TestPlugin(reader, i) for i in range(1, 10)]
            for plugin in test_plugins:
                plugin.run()

            with IndexWriter(index_dir) as writer:
                for i, plugin in enumerate(test_plugins):
                    writer.set_plugin_state(plugin)

            assert len(reader.list_plugins()) == 9

        # Restore the plugin from the index
        with IndexReader(index_dir) as reader:
            restore_plugin = TestPlugin(reader, 1)
            restore_plugin.load()
            assert test_plugins[0].internal_state == restore_plugin.internal_state

        # Raise an error if plugin's name-settings combination not found
        with pytest.raises(PluginNotFoundError):
            with IndexReader(index_dir) as reader:
                plugin_not_stored = TestPlugin(reader, 10)
                plugin_not_stored.load()

        # Delete plugin data from the index
        with IndexWriter(index_dir) as writer:
            writer.delete_plugin_instance(restore_plugin)

        with IndexReader(index_dir) as reader:
            assert len(reader.list_plugins()) == 8

        with IndexWriter(index_dir) as writer:
            writer.delete_plugin(plugin_name='trivial_test_plugin')

        with IndexReader(index_dir) as reader:
            assert len(reader.list_plugins()) == 0


def test_plugin_abc():
    """This is pointless but necessary to get 100% coverage :("""
    abstract_method_tester(AnalyticsPlugin)
    assert True
