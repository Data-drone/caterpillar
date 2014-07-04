# Copyright (C) Kapiche
# Author: Ryan Stuart <ryan@kapiche.com>, Kris Rogers <kris@kapiche.com>
"""Tests for plugin module."""
import os

from caterpillar import abstract_method_tester
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import Index
from caterpillar.processing.plugin import AnalyticsPlugin
from caterpillar.processing import schema


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


def test_plugin():
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        analyser = DefaultAnalyser(stopword_list=stopwords.ENGLISH_TEST)
        index = Index.create(schema.Schema(text=schema.TEXT(analyser=analyser)))
        index.add_document(text=data, frame_size=2)
        index.run_plugin(TrivialTestPlugin)
        assert int(index.get_plugin_data(TrivialTestPlugin, 'fake_data')['a']) == 100
        index.run_plugin(TrivialTestPlugin)  # Overwrite plugin results


def test_plugin_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(AnalyticsPlugin)
    assert 'Your mum' != 'Awesome'  # While we are writing pointless code we may as well really make it pointless
