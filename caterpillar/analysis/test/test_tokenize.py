# caterpillar: Tests for the caterpillar.analysis.tokenize module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>

import os
from caterpillar.analysis.tokenize import ParagraphTokenizer


def test_paragraph_tokenizer_alice():
    with open(os.path.abspath('caterpillar/analysis/test/alice_test_data.txt'), 'r') as f:
        data = f.read()
        paragraphs = ParagraphTokenizer().tokenize(data)
        assert len(paragraphs) == 22


def test_paragraph_tokenizer_economics():
    with open(os.path.abspath('caterpillar/analysis/test/economics_test_data.txt'), 'r') as f:
        data = f.read()
        paragraphs = ParagraphTokenizer().tokenize(data)
        assert len(paragraphs) == 4