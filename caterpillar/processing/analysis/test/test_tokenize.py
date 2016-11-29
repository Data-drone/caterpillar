# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Tests for the caterpillar.processing.analysis.tokenize module."""
import os
import pytest
from arrow.parser import ParserError
from itertools import product
from caterpillar.processing.analysis.tokenize import *


# Some basic error condition and plumbing tests #
def test_tokenizer_base_class():
    tokenizer = Tokenizer()

    with pytest.raises(NotImplementedError):
        tokenizer.tokenize("Some text")


def test_regex_tokenizer_invalid_pattern():
    with pytest.raises(ValueError):
        RegexpTokenizer('*&&jkbbj&&')  # An invalid regex


# Actually test the tokenizers #
def test_paragraph_tokenizer_alice():
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        count = 0
        for p in ParagraphTokenizer().tokenize(data):
            count += 1
        assert count == 25


def test_paragraph_tokenizer_economics():
    with open(os.path.abspath('caterpillar/test_resources/economics_test_data.txt'), 'r') as f:
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
    tokens = WordTokenizer().tokenize(
        "But John McGee was sure to kneel before him. The King of Scotland was a rash man.")
    words = []
    for t in tokens:
        words.append(t.value)
    assert words == ['But', 'John McGee', 'was', 'sure', 'to', 'kneel', 'before', 'him',
                     'The', 'King of Scotland', 'was', 'a', 'rash', 'man']


def test_word_tokenizer_bush():
    with open(os.path.abspath('caterpillar/test_resources/bush_test_data.txt'), 'r') as f:
        data = f.read()
        tokens = WordTokenizer().tokenize(data)
        words = []
        for t in tokens:
            words.append(t.value)
        assert words[-1] == 'Applause'
        assert len(words) == 75


def test_word_tokenizer_economics():
    with open(os.path.abspath('caterpillar/test_resources/economics_test_data.txt'), 'r') as f:
        data = f.read()
        tokens = WordTokenizer().tokenize(data.decode('utf-8'))
        words = []
        for t in tokens:
            words.append(t.value)
        assert len(words) == 311


def test_word_tokenizer_email_simple():
    tokens = WordTokenizer().tokenize(
        "A test sentence with the email adress John_Smith@domain123.org.au embedded in it.")
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


def test_word_tokenizer_url():
    wt = WordTokenizer()
    url1 = "https://www.facebook.com"
    assert url1 in [t.value for t in wt.tokenize("A sample url {} .".format(url1))]
    url2 = "http://twitter.com/@test"
    assert url2 in [t.value for t in wt.tokenize("A sample url {} .".format(url2))]
    url3 = "https://www.google.com.au/?gfe_rd=cr&ei=TWL8UuK1KKuN8Qf48oHgBg"
    assert url3 in [t.value for t in wt.tokenize("A sample url {} .".format(url3))]
    url4 = "www.test.io/?q=123"
    assert url4 in [t.value for t in wt.tokenize("A sample url {} .".format(url4))]
    not_url = "www house cleaning"
    assert len(list(wt.tokenize(not_url))) == 3


def test_everything_tokenizer():
    token = list(EverythingTokenizer().tokenize("Test"))[0]
    assert token.value == 'Test'

    # Now with unicode:
    test = u"\u2019"
    token = list(EverythingTokenizer().tokenize(test))[0]
    assert token.value == test


def test_datetime_tokenizer():
    # Construct a cross product of the same time in different formats
    date_format_examples = ['DD/MM/YYYY', 'MM/DD/YYYY', 'YYYY-MM-DD']
    time_format_examples = ['HH:mm:ss', 'HH:mm', 'H:mm a', 'H:mm:ssA']
    date_examples = ['21/02/2014', '02/21/2014', '2014-02-21']
    time_examples = ['13:00:00', '13:00', '1:00 pm', '1:00:00PM']

    canonical_repr_utc = '2014-02-21T13:00:00z'
    canonical_repr_naive = canonical_repr_utc[:-1]

    utc_offsets = ['+0{}:00'.format(i) for i in range(10)]
    offset_format = ['ZZ'] * 10
    offset_canonical_repr = ['2014-02-21T{:02}:00:00z'.format(13 - i) for i in range(10)]

    with pytest.raises(ParserError):
        next(DateTimeTokenizer().tokenize('Not a date'))

    # Test ignore_tz
    for dt_format, dt_string in zip(product(date_format_examples, time_format_examples),
                                    product(date_examples, time_examples)):
        dt_tokenizer = DateTimeTokenizer(datetime_formats=[' '.join(dt_format)], ignore_tz=True)
        value = next(dt_tokenizer.tokenize(' '.join(dt_string))).value
        assert value == canonical_repr_naive

    # Test with timezone:
    for dt_format, dt_string in zip(product(date_format_examples, time_format_examples),
                                    product(date_examples, time_examples)):
        dt_tokenizer = DateTimeTokenizer(datetime_formats=[' '.join(dt_format)], ignore_tz=False)
        value = next(dt_tokenizer.tokenize(' '.join(dt_string))).value
        assert value == canonical_repr_utc

    # Test with UTC offsets in the import string
    for dt_format, dt_string, canonical_repr in zip(product(date_format_examples, time_format_examples, offset_format),
                                                    product(date_examples, time_examples, utc_offsets),
                                                    product(date_examples, time_examples, offset_canonical_repr)):
        dt_tokenizer = DateTimeTokenizer(datetime_formats=[' '.join(dt_format)], ignore_tz=False)
        value = next(dt_tokenizer.tokenize(' '.join(dt_string))).value
        assert value == canonical_repr[2]
