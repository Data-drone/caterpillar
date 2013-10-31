# caterpillar: Tests for scoring module
#
# Copyright (C) 2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
from caterpillar import abstract_method_tester
from caterpillar.searching.scoring import Scorer


def test_scoring_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(Scorer)
