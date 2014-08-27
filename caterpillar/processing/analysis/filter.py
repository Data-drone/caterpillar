# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Tools for filtering tokens."""
import regex


class Filter(object):
    """
    Base class for Filter objects.

    A Filter subclass must implement a filter() method that takes a single argument, which is an iterator of Token
    objects, and yield a series of Token objects in return.

    """
    def filter(self, tokens):
        """
        Return filtered tokens from the tokens iterator.

        """
        raise NotImplementedError


class PassFilter(Filter):
    """
    An identity filter: passes the tokens through untouched.

    """
    def filter(self, tokens):
        return tokens


class LowercaseFilter(Filter):
    """
    Uses unicode.lower() to lowercase token text.

    >>> rext = RegexTokenizer()
    >>> stream = rext("This is a TEST")
    >>> [token.value for token in LowercaseFilter(stream)]
    ["this", "is", "a", "test"]

    """
    def filter(self, tokens):
        for t in tokens:
            t.value = t.value.lower()
            yield t


class PositionalLowercaseWordFilter(Filter):
    """
    Uses unicode.lower() to lowercase single word tokens that appear to be titles and are in a certain position.

    Only applies the filter if the token only contains only 1 word (ie no spaces) and unicode.istitle() returns true.
    Used internally to force the case of words appearing at the start of a sentence to lower if it isn't part of a
    compound name.

    >>> rext = RegexTokenizer()
    >>> stream = rext("This is a TEST")
    >>> [token.value for token in LowercaseFilter(0).filter()]
    ["this", "is", "a", "TEST"]

    Required Arguments:
    position -- A 0 based int index the token needs to be in to apply this filter.

    """
    def __init__(self, position):
        self._position = position

    def filter(self, tokens):
        for t in tokens:
            if t.position == self._position and t.value.istitle() and ' ' not in t.value:
                t.value = t.value.lower()
            yield t


class StopFilter(Filter):
    """
    Marks "stop" words (words too common to index) in the stream.

    >>> rext = RegexTokenizer()
    >>> stream = rext("this is a test")
    >>> stopper = StopFilter()
    >>> [token.value for token in stopper(stream)]
    ["this", "test"]

    Required Arguments
    stoplist -- A list of stop words lower cased.

    Optional Arguments
    minsize -- An int indicating the smallest acceptable word length.

    """
    def __init__(self, stoplist, minsize=3):
        self._stoplist = {s: None for s in stoplist}
        self._minsize = minsize

    def filter(self, tokens):
        for t in tokens:
            if len(t.value) < self._minsize or t.value.lower() in self._stoplist:
                t.stopped = True
            yield t


class BiGramFilter(Filter):
    """
    Identifies bi-grams in a token stream from a given list.

    >>> rext = RegexTokenizer()
    >>> stream = rext("this is a bigram")
    >>> ngrams = BiGramFilter(["a bigram"])
    >>> [token.value for token in bigrams(stream)]
    ["this", "is, "a bigram"]

    Required Arguments
    bi_grams -- A list of n-gram strings to match.

    """
    def __init__(self, bi_grams):
        self._bi_grams = bi_grams

    def filter(self, tokens):
        prev_token = None
        for t in tokens:
            if t.value[0].isupper() or t.stopped:
                # Names and stopwords can't be bi-grams
                if prev_token:
                    yield prev_token
                    prev_token = None
                yield t
                continue
            elif prev_token:
                bi_gram = u"{} {}".format(prev_token.value, t.value)
                if bi_gram in self._bi_grams:
                    yield t.update(bi_gram, position=prev_token.position, index=(prev_token.index[0], t.index[1]))
                    prev_token = None
                    continue
                else:
                    yield prev_token
            prev_token = t.copy()
        if prev_token:  # Don't forget the last token!!
            yield prev_token


class PotentialBiGramFilter(Filter):
    """
    Identifies bi-grams in a token stream along with regular tokens.

    Potential bi-grams won't include stopped tokens or names.

    WARNING: This filter differs from most other filters in that it returns a list of token objects, not just single
    tokens. This is done purely for performance. If it finds a non-bi-gram, a list of just a single token is returned.

    """
    def filter(self, tokens):
        prev_token = None
        for t in tokens:
            if t.value[0].isupper() or t.stopped:
                # Skip names and stopwords
                if prev_token:
                    yield [prev_token]
                    prev_token = None
                yield [t]
                continue
            elif prev_token:
                yield [prev_token, t]
            prev_token = t.copy()
        if prev_token:
            yield [prev_token]


class SubstitutionFilter(Filter):
    """
    Performs a regular expression substitution on the token text.

    This is especially useful for removing text from tokens, for example hyphens::

        ana = RegexTokenizer(r"\\S+") | SubstitutionFilter("-", "")

    Because it has the full power of the ``regex.sub()`` method behind it, this filter can perform some fairly complex
    transformations. For example, to take tokens like ``'a=b', 'c=d', 'e=f'`` and change them to ``'b=a', 'd=c',
    'f=e'``::

    >>> sf = SubstitutionFilter("([^/]*)/(./*)", r"\\2/\\1")

    Required Arguments
    pattern -- A pattern string or compiled regular expression object describing the text to replace.
    replacement -- A string of substitution text.

    """
    def __init__(self, pattern, replacement):
        self.pattern = regex.compile(pattern)
        self.replacement = replacement

    def filter(self, tokens):
        pattern = self.pattern
        replacement = self.replacement

        for t in tokens:
            t.value = pattern.sub(replacement, t.value)
            yield t
