# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Tests for the caterpillar.processing.analysis.filter module."""
import pytest
from caterpillar.processing.analysis.filter import (
    StopFilter, PassFilter, Filter, SubstitutionFilter, LowercaseFilter, SearchFilter,
    OuterPunctuationFilter, PossessiveContractionFilter
)
from caterpillar.processing.analysis.tokenize import WordTokenizer, Token, SimpleWordTokenizer


TEST_STRING = 'This is my test-string. Isn\'t it great?'
TOKENIZER = WordTokenizer()


# Some basic error condition and plumbing tests #
def test_filter_base_class():
    f = Filter()

    with pytest.raises(NotImplementedError):
        f.filter([Token()])


# Functional tests #
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


def test_search_filter():
    f = SearchFilter('i')
    tokens = TOKENIZER.tokenize(TEST_STRING)

    for t in f.filter(tokens):
        if t.position in (0, 1, 4, 6):
            assert t.value == 'i'


def test_outerpunctuation_filter():
    f = OuterPunctuationFilter(leading_allow=['@#$'], trailing_allow=['/%!'])
    tokens = f.filter(SimpleWordTokenizer().tokenize('@!@$#te--st/%!!-!! --@t@@ --t!!@ --tc-a! -tca!'))

    token = next(tokens).value
    assert token == '@$#te--st/%!!'

    token = next(tokens).value
    assert token == '@t'

    token = next(tokens).value
    assert token == 't!!'

    token = next(tokens).value
    assert token == 'tc-a!'

    token = next(tokens).value
    assert token == 'tca!'


def test_possessivecontraction_filter():
    f = PossessiveContractionFilter()
    tokens = f.filter(SimpleWordTokenizer().tokenize(
        u"bob's bob\u2019s bob\u02BCs bob\u02BBs bob\u055As bob\uA78Bs bob\uA78Cs bob\uFF07s"
    ))

    for t in tokens:
        assert t.value == 'bob'
