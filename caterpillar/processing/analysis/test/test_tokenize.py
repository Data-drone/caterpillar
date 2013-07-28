# caterpillar: Tests for the caterpillar.processing.analysis.tokenize module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import os
import pytest
from caterpillar.processing.analysis.tokenize import *


#### Some basic error condition and plumbing tests ####
def test_tokenizer_base_class():
    tokenizer = Tokenizer()

    with pytest.raises(NotImplementedError):
        tokenizer.tokenize("Some text")


def test_regex_tokenizer_invalid_pattern():
    with pytest.raises(ValueError):
        RegexpTokenizer('*&&jkbbj&&')  # An invalid regex


#### Actually test the tokenizers ####
def test_paragraph_tokenizer_alice():
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        count = 0
        for p in ParagraphTokenizer().tokenize(data):
            count += 1
        assert count == 22


def test_paragraph_tokenizer_economics():
    with open(os.path.abspath('caterpillar/resources/economics_test_data.txt'), 'r') as f:
        data = f.read()
        count = 0
        for p in ParagraphTokenizer().tokenize(data):
            count += 1
        assert count == 4


def test_word_tokenizer_tags():
    tokens = WordTokenizer().tokenize("--#Hello, this is a #tweet... It was made by @me!")
    words = []
    for t in tokens:
        words.append(t.value)
    assert words == ['#Hello', 'this', 'is', 'a', '#tweet', 'It', 'was', 'made', 'by', '@me']


def test_word_tokenizer_contractions():
    tokens = WordTokenizer().tokenize("I've observed that it wasn't the dog's fault.")
    words = []
    for t in tokens:
        words.append(t.value)
    assert words == ["I've", "observed", "that", "it", "wasn't", "the", "dog", "s", "fault"]


def test_word_tokenizer_names():
    tokens = WordTokenizer().tokenize("But John McGee was sure to kneel before him. The King of Scotland was a rash man.")
    words = []
    for t in tokens:
        words.append(t.value)
    assert words == ['But', 'John McGee', 'was', 'sure', 'to', 'kneel', 'before', 'him', 'The', 'King of Scotland', 'was', 'a', 'rash', 'man']


def test_word_tokenizer_bush():
    with open(os.path.abspath('caterpillar/resources/bush_test_data.txt'), 'r') as f:
        data = f.read()
        tokens = WordTokenizer().tokenize(data)
        words = []
        for t in tokens:
            words.append(t.value)
        assert words[-1] == 'Applause'
        assert len(words) == 75


def test_word_tokenizer_economics():
    with open(os.path.abspath('caterpillar/resources/economics_test_data.txt'), 'r') as f:
        data = f.read()
        tokens = WordTokenizer().tokenize(data.decode('utf-8'))
        words = []
        for t in tokens:
            words.append(t.value)
        assert len(words) == 311


def test_word_tokenizer_email_simple():
    tokens = WordTokenizer().tokenize("A test sentence with the email adress John_Smith@domain123.org.au embedded in it.")
    words = []
    for t in tokens:
        words.append(t.value)
    assert words[7] == 'John_Smith@domain123.org.au'


def test_word_tokenizer_email_hard():
    tokens = WordTokenizer().tokenize("Another example with disposable.style.email.with+symbol@example.com.")
    words = []
    for t in tokens:
        words.append(t.value)
    assert words[-1] == 'disposable.style.email.with+symbol@example.com'


def test_word_tokenizer_number():
    tokens = WordTokenizer().tokenize("A sentence with numbers 1, 100,000, 100,000,000.123 and $50.")
    words = []
    for t in tokens:
        words.append(t.value)
    assert len(words) == 9
    assert words[6] == '100,000,000.123'