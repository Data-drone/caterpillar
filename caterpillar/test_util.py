# Copyright (c) 2012-2015 Kapiche Ltd.
# Author: Ryan Stuart<ryan@kapiche.com>
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.analyse import Analyser
from caterpillar.processing.analysis.filter import OuterPunctuationFilter, StopFilter, PositionalLowercaseWordFilter, \
    BiGramFilter
from caterpillar.processing.analysis.filter import PossessiveContractionFilter
from caterpillar.processing.analysis.tokenize import SimpleWordTokenizer


class TestAnalyser(Analyser):
    _tokenizer = SimpleWordTokenizer(detect_compound_names=True)

    def __init__(self, stopword_list=None):
        super(TestAnalyser, self).__init__()
        if stopword_list is None:
            stopword_list = stopwords.ENGLISH_TEST

        self._filters = [
            OuterPunctuationFilter(leading_allow=['@', '#']),
            PossessiveContractionFilter(),
            StopFilter(stopword_list, minsize=stopwords.MIN_WORD_SIZE),
            PositionalLowercaseWordFilter(0),
        ]

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters


class TestBiGramAnalyser(Analyser):
    _tokenizer = SimpleWordTokenizer(detect_compound_names=True)

    def __init__(self, bi_grams, stopword_list=None):
        if stopword_list is None:
            stopword_list = stopwords.ENGLISH_TEST
        self._filters = [
            OuterPunctuationFilter(leading_allow=['@', '#']),
            PossessiveContractionFilter(),
            StopFilter(stopword_list, minsize=stopwords.MIN_WORD_SIZE),
            PositionalLowercaseWordFilter(0),
            BiGramFilter(bi_grams),
        ]

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters
