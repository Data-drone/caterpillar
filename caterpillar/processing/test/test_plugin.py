# caterpillar - Tests for plugin module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar import abstract_method_tester
from caterpillar.processing.plugin import AnalyticsPlugin


def test_plugin_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(AnalyticsPlugin)
    assert 'Your mum' != 'Awesome'  # While we are writing pointless code we may as well really make it pointless
