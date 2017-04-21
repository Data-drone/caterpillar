# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Tools to perform analysis of text streams (aka tokenizing and filtering)."""
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.filter import (
    StopFilter, PositionalLowercaseWordFilter, OuterPunctuationFilter, PossessiveContractionFilter
)
from caterpillar.processing.analysis.tokenize import EverythingTokenizer, \
    SimpleWordTokenizer, DateTimeTokenizer


class Analyser(object):
    """
    Abstract base class for an analyser.

    All analysers are a combination of a tokenizer and 0 or more filters. This class accesses the tokenizer by calling
    self.get_tokenizer() and the filters via self.get_filters(). You need to implement the get_tokenizer() method at
    a minimum.

    This class also defines the analyse() method which will call the tokenizer followed by the filters in order before
    finally returning the token.

    """
    def analyse(self, value):
        # Tokenize first
        token = self.get_tokenizer().tokenize(value)
        # Then filter
        if self.get_filters():
            for f in self.get_filters():
                token = f.filter(token)
        return token

    def get_tokenizer(self):
        raise NotImplementedError

    def get_filters(self):
        return None


class DefaultAnalyser(Analyser):
    """
    The default caterpillar ``Analyser`` which mostly splits on whitespace and punctuation, except for a few special
    cases, and removes stopwords.

    This analyzer uses a ``WordTokenizer`` in combination with a ``StopFilter`` and a ``PositionalLowercaseWordFilter``.

    Optional Arguments:
    stopword_list -- A list of stop words to override the default ``stopwords.ENGLISH`` one.

    """
    _tokenizer = SimpleWordTokenizer(detect_compound_names=True)

    def __init__(self, stopword_list=tuple(), min_word_size=1):
        super(DefaultAnalyser, self).__init__()
        if stopword_list is None:
            stopword_list = stopwords.ENGLISH

        self._filters = [
            OuterPunctuationFilter(leading_allow=['@', '#']),
            PossessiveContractionFilter(),
            StopFilter(stopword_list, minsize=min_word_size),
            PositionalLowercaseWordFilter(0),
        ]

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters


class EverythingAnalyser(Analyser):
    """
    A EverythingAnalyser just returns the entire input string as a token.

    """
    _tokenizer = EverythingTokenizer()
    _filters = []

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters


class DateTimeAnalyser(Analyser):
    """
    Analyser for ``DATETIME`` fields. Returns an ISO8601 format datetime string.

    """
    _filters = []

    def __init__(self, datetime_formats=None, ignore_tz=False):
        self.datetime_formats = datetime_formats
        self.ignore_tz = ignore_tz
        self._tokenizer = DateTimeTokenizer(datetime_formats, ignore_tz)

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters
