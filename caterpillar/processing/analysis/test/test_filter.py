# caterpillar: Tests for the caterpillar.processing.analysis.filter module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import pytest
from caterpillar.processing.analysis.filter import *
from caterpillar.processing.analysis.tokenize import WordTokenizer, Token


TEST_STRING = 'This is my test-string. Isn\'t it great?'
TOKENIZER = WordTokenizer()


#### Some basic error condition and plumbing tests ####
def test_filter_base_class():
    f = Filter()

    with pytest.raises(NotImplementedError):
        f.filter([Token()])


#### Functional tests ####
def test_stop_filter():
    f = StopFilter(['is', 'it'], 2)
    tokens = TOKENIZER.tokenize(TEST_STRING)

    for t in f.filter(tokens):
        if t.position in [1, 6]:
            assert t.stopped


def test_pass_filter():
    f = PassFilter()
    tokens = TOKENIZER.tokenize(TEST_STRING)

    count = 0
    for t in f.filter(tokens):
        if t.position == 3:
            assert t.value == 'test'
        count += 1
    assert count == 8


def test_sub_filter():
    f = SubstitutionFilter('string', 'ping')
    tokens = TOKENIZER.tokenize(TEST_STRING)

    for t in f.filter(tokens):
        if t.position == 4:
            assert t.value == 'ping'


def test_lower_filter():
    f = LowercaseFilter()
    tokens = TOKENIZER.tokenize(TEST_STRING)

    for t in f.filter(tokens):
        if t.position == 0:
            assert t.value == 'this'