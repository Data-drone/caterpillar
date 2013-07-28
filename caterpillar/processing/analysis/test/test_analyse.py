# caterpillar: Tests for the caterpillar.processing.analysis.analyse module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import pytest
from caterpillar.processing.analysis.analyse import Analyser, DefaultAnalyser
from caterpillar.processing.analysis.tokenize import Tokenizer


#### Some basic error condition and plumbing tests ####
def test_base_analyse_class():
    analyser = Analyser()

    with pytest.raises(NotImplementedError):
        analyser.analyse('Some text')
    assert analyser.get_filters() is None


def test_default_analyser():
    analyser = DefaultAnalyser()

    assert len(analyser.get_filters()) == 2
    assert isinstance(analyser.get_tokenizer(), Tokenizer)