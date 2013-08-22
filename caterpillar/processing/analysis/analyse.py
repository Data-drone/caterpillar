# caterpillar: Tools to perform analysis of text streams (aka tokenizing and filtering)
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import logging
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.filter import StopFilter, PositionalLowercaseWordFilter, BiGramFilter, PotentialBiGramFilter
from caterpillar.processing.analysis.tokenize import WordTokenizer, EverythingTokenizer


logger = logging.getLogger(__name__)


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

    """
    _tokenizer = WordTokenizer(detect_compound_names=True)
    _filters = [StopFilter(stopwords.ENGLISH, minsize=stopwords.MIN_WORD_SIZE), PositionalLowercaseWordFilter(0)]

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters


class DefaultTestAnalyser(Analyser):
    """
    The default caterpillar test ``Analyser`` which mostly splits on whitespace and punctuation except for a few special
    cases and removes a small subset of stopwords.

    This analyser uses a ``WordTokenizer`` in combination with a ``StopFilter`` and a ``PositionalLowercaseWordFilter``.
    It uses a stopword list that never changes to reduce the amount of testing updates required when updating the
    stoplist.

    """
    _tokenizer = WordTokenizer(detect_compound_names=True)
    _filters = [StopFilter(stopwords.ENGLISH_TEST, minsize=stopwords.MIN_WORD_SIZE), PositionalLowercaseWordFilter(0)]

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters


class BiGramAnalyser(Analyser):
    """
    A bi-gram ``Analyser`` that behaves exactly like the ``DefaultAnalyser`` except it also makes use of a
    ``BiGramFilter``.

    This analyser uses a ``WordTokenizer`` in combination with a ``StopFilter``, ``PositionalLowercaseWordFilter`` and a
    ``BiGramFilter``.

    Required Arguments
    bi_grams -- a list of string n-grams to match. Passed directly to ``BiGramFilter``.

    """
    _tokenizer = WordTokenizer(detect_compound_names=True)

    def __init__(self, bi_grams):
        self._filters = [StopFilter(stopwords.ENGLISH, minsize=stopwords.MIN_WORD_SIZE),
                         PositionalLowercaseWordFilter(0), BiGramFilter(bi_grams)]

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters


class BiGramTestAnalyser(Analyser):
    """
    Same as ``BiGramAnalyser`` but uses a fixed stoplist that never changes.

    """
    _tokenizer = WordTokenizer(detect_compound_names=True)

    def __init__(self, bi_grams):
        self._filters = [StopFilter(stopwords.ENGLISH_TEST, minsize=stopwords.MIN_WORD_SIZE),
                         PositionalLowercaseWordFilter(0), BiGramFilter(bi_grams)]

    def get_tokenizer(self):
        return self._tokenizer

    def get_filters(self):
        return self._filters


class PotentialBiGramAnalyser(Analyser):
    """
    A PotentialBiGramAnalyser returns a list of possible bi-grams from a stream.

    This analyser uses a ``WordTokenizer`` in combination with a ``StopFilter``, ``PositionalLowercaseWordFilter`` and a
    ``PotentialBiGramFilter`` to generate a stream of possible bi-grams.

    """
    _tokenizer = WordTokenizer(detect_compound_names=True)
    _filters = [StopFilter(stopwords.ENGLISH, minsize=stopwords.MIN_WORD_SIZE), PositionalLowercaseWordFilter(0),
                PotentialBiGramFilter()]

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