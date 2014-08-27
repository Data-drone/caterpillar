# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>
"""Tests for scoring module."""
from caterpillar import abstract_method_tester
from caterpillar.searching.scoring import Scorer


def test_scoring_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(Scorer)
