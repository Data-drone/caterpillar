# caterpillar: Tests for the caterpillar.processing.tokenize module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>

import os
import pytest
from caterpillar.processing.tokenize import *


#### Some basic error condition and plumbing tests ####
def test_regex_tokenizer_invalid_pattern():
    with pytest.raises(ValueError):
        NewRegexpTokenizer('*&&jkbbj&&')  # An invalid regex


def test_token_filter_base_cls():
    with pytest.raises(NotImplementedError):
        TokenFilter().filter([])  # Base class, can't instantiate it.


def test_regex_tokenizer_repr():
    assert 'NewRegexpTokenizer(pattern=\w, gaps=False, flags=312)' == NewRegexpTokenizer('\w').__repr__()


#### Actually test the tokenizers ####
def test_paragraph_tokenizer_alice():
    with open(os.path.abspath('caterpillar/processing/test/alice_test_data.txt'), 'r') as f:
        data = f.read()
        paragraphs = ParagraphTokenizer().tokenize(data)
        assert len(paragraphs) == 22


def test_paragraph_tokenizer_offsets_alice():
    with open(os.path.abspath('caterpillar/processing/test/alice_test_data.txt'), 'r') as f:
        data = f.read()
        offsets = list(ParagraphTokenizer().span_tokenize(data))
        assert len(offsets) == 22


def test_paragraph_tokenizer_economics():
    with open(os.path.abspath('caterpillar/processing/test/economics_test_data.txt'), 'r') as f:
        data = f.read()
        paragraphs = ParagraphTokenizer().tokenize(data)
        assert len(paragraphs) == 4


def test_word_tokenizer_tags():
    words = WordTokenizer().tokenize("--#Hello, this is a #tweet... It was made by @me!")
    assert words == ['#Hello', 'this', 'is', 'a', '#tweet', 'It', 'was', 'made', 'by', '@me']


def test_word_tokenizer_contractions():
    words = WordTokenizer().tokenize("I've observed that it wasn't the dog's fault.")
    assert words == ["I've", "observed", "that", "it", "wasn't", "the", "dog", "s", "fault"]


def test_word_tokenizer_names():
    words = WordTokenizer().tokenize("But John was sure to kneel before him. The King of Scotland was a rash man.")
    assert words == ['But', 'John', 'was', 'sure', 'to', 'kneel', 'before', 'him', 'The', 'King of Scotland', 'was', 'a', 'rash', 'man']


def test_word_tokenizer_bush():
    with open(os.path.abspath('caterpillar/processing/test/bush_test_data.txt'), 'r') as f:
        data = f.read()
        words = WordTokenizer().tokenize(data)
        assert words[-1] == 'Applause'
        assert len(words) == 75


def test_word_tokenizer_economics():
    with open(os.path.abspath('caterpillar/processing/test/economics_test_data.txt'), 'r') as f:
        data = f.read()
        words = WordTokenizer().tokenize(data.decode('utf-8'))
        assert len(words) == 311


def test_word_tokenizer_email():
    words = WordTokenizer().tokenize("A test sentence with the email adress John_Smith@domain123.org.au embedded in it.")
    assert words[7] == 'John_Smith@domain123.org.au'
    words = WordTokenizer().tokenize("Another example with disposable.style.email.with+symbol@example.com.")
    assert words[-1] == 'disposable.style.email.with+symbol@example.com'


def test_word_tokenizer_number():
    words = WordTokenizer().tokenize("A sentence with numbers 1, 100,000, 100,000,000.123 and $50.")
    assert len(words) == 9
    assert words[6] == '100,000,000.123'