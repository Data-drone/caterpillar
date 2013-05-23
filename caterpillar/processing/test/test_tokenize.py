# caterpillar: Tests for the caterpillar.processing.tokenize module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>

import os
from caterpillar.processing.tokenize import ParagraphTokenizer, WordTokenizer


def test_paragraph_tokenizer_alice():
    with open(os.path.abspath('caterpillar/processing/test/alice_test_data.txt'), 'r') as f:
        data = f.read()
        paragraphs = ParagraphTokenizer().tokenize(data)
        assert len(paragraphs) == 22


def test_paragraph_tokenizer_economics():
    with open(os.path.abspath('caterpillar/processing/test/economics_test_data.txt'), 'r') as f:
        data = f.read()
        paragraphs = ParagraphTokenizer().tokenize(data)
        assert len(paragraphs) == 4

def test_word_tokenizer_():
    words = WordTokenizer().tokenize("--#Hello, this is a #tweet... It was made by @me!")
    assert words == ['#Hello', 'this', 'is', 'a', '#tweet', 'It', 'was', 'made', 'by', '@me']

def test_word_tokenizer_bush():
    with open(os.path.abspath('caterpillar/processing/test/bush_test_data.txt'), 'r') as f:
        data = f.read()
        words = WordTokenizer().tokenize(data)
        assert words[-1] == 'Applause'
        assert len(words) == 81
