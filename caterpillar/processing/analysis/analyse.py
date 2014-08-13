# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""
For fields to get into an index they need to be analysed. Analysing is a three step process:

    1. Preprocess the data - see :mod:`caterpillar.processing.analysis.preprocess`.
    2. Tokenize the data - see :mod:`caterpilalr.processing.analysis.tokenize`.
    3. Filter the data - see :mod:`caterpillar.processing.analysis.filter`.

Preprocessing is used to transform the input data. For example, the FramePreprocessor adds sentence and paragraph
boundary markers into the input text for use in the tokenization process. A tokeniser is responsible for turning the
input into :class:`Token <caterpillar.processing.analysis.tokenize.Token`s. Some tokenisers just return 1 token, others
return a stream of tokens. Finally, the tokens returned tokenizers can be filtered by 1 or more filters. Filters can
do things like remove stopwords etc.

Combining all these steps into one callable object is the job of an :class:`Analyser`.

"""
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.filter import StopFilter, PositionalLowercaseWordFilter, BiGramFilter, \
    PotentialBiGramFilter
from caterpillar.processing.analysis.preprocess import FramePreprocessor
from caterpillar.processing.analysis.tokenize import WordTokenizer, EverythingTokenizer


class Analyser(object):
    """
    Abstract base class for an analyser.

    All analysers are a combination of a :mod:`preprocess <caterpiller.processing.analysis.preprocess>`or (optional), a
    :mod:`tokenize <caterpillar.processing.analysis.tokenize>`r and 0 or more
    :mod:`filter <caterpillar.processing.analysis.filter>`s. This class accesses the preprocessor via
    :attr:`.preprocessor` (can return None), tokenizer via :attr:`.tokenizer` and the filters via :attr:`filters`
    (returns a list). You need to supply at minimum a tokenizer.

    This class also defines the :meth:`.analyse` method which will call the preprocessor followed by the tokenizer
    followed by  the filters in order before finally returning the token generator. Calling this class directly just
    calls :meth:`.analyse`.

    """
    def __init__(self, tokenizer, preprocessor=None, filters=None):
        self._tokenizer = tokenizer
        self._preprocessor = preprocessor
        self._filters = filters or []

    def __call__(self, value):
        return self.analyse(value)

    def analyse(self, value):
        # Preprocessor first
        if self.preprocessor:
            value = self.preprocessor(value)
        # Tokenize first
        token_gen = self.tokenizer(value)
        # Then filter
        for f in self.filters:
            token_gen = f(token_gen)
        return token_gen

    @property
    def preprocessor(self):
        return self._preprocessor

    @property
    def tokenizer(self):
        return self._tokenizer

    @property
    def filters(self):
        return self._filters


class DefaultAnalyser(Analyser):
    """
    The default caterpillar :class:`Analyser` which mostly splits on whitespace and punctuation, except for a
    few special cases, and removes stopwords.

    This analyzer uses a :class:`WordTokenizer <caterpillar.processing.analysis.tokenize.WordTokenizer>` in combination
    with a :class:`FramePreprocessor <caterpillar.processing.analysis.preprocess.FramePreprocessor>`, a
    :class:`StopFilter <caterpillar.processing.analysis.filter.StopFilter>` and a
    :class:`PositionalLowercaseWordFilter <caterpillar.processing.analysis.filter.PositionalLowercaseWordFilter>`.

    """

    def __init__(self, frame_size, stopword_list=None):
        if stopword_list is None:
            stopword_list = stopwords.ENGLISH
        super(DefaultAnalyser, self).__init__(WordTokenizer(), preprocessor=FramePreprocessor(frame_size),
                                              filters=[StopFilter(stopword_list, minsize=stopwords.MIN_WORD_SIZE)])


class BiGramAnalyser(Analyser):
    """
    A bi-gram ``Analyser`` that behaves exactly like the ``DefaultAnalyser`` except it also makes use of a
    ``BiGramFilter``.

    This analyser uses a ``WordTokenizer`` in combination with a ``StopFilter``, ``PositionalLowercaseWordFilter`` and a
    ``BiGramFilter``.

    Required Arguments
    bi_grams -- a list of string n-grams to match. Passed directly to ``BiGramFilter``.

    Optional Arguments:
    stopword_list -- A list of stop words to override the default English one.

    """
    _tokenizer = WordTokenizer(detect_compound_names=True)

    def __init__(self, bi_grams, stopword_list=None):
        if stopword_list is None:
            stopword_list = stopwords.ENGLISH
        self._filters = [StopFilter(stopword_list, minsize=stopwords.MIN_WORD_SIZE),
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
