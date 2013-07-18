# caterpillar: Tools to tokenize text
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import logging
from nltk.internals import convert_regexp_to_nongrouping
from nltk.tokenize.api import TokenizerI

import regex


logger = logging.getLogger(__name__)


class NewRegexpTokenizer(TokenizerI):
    """
    A tokenizer that splits a string using a regular expression.

    This class can be used to match either the tokens or the separators between tokens.

        >>> tokenizer = NewRegexpTokenizer('\w+|\$[\d\.]+|\S+')

    This class is basically a copy of ''ntlk.RegexpRokenizer'' except that we use the new python regex module instead
    of the existing re module. This means unicode codepoint properties are supported.

        >>> tokenizer = NewRegexpTokenizer('\w+|\$[\p{N}\.]+|\S+')

    :type pattern: str
    :param pattern: The pattern used to build this tokenizer. (This pattern may safely contain grouping parentheses.)
    :type gaps: bool
    :param gaps: True if this tokenizer's pattern should be used to find separators between tokens; False if this
        tokenizer's pattern should be used to find the tokens themselves.
    :type flags: int
    :param flags: The regexp flags used to compile this tokenizer's pattern.  By default, the following flags are
        used: `regex.UNICODE | regex.MULTILINE | regex.DOTALL | regex.VERSION1`.

    """
    def __init__(self, pattern, gaps=False, flags=regex.UNICODE | regex.MULTILINE | regex.DOTALL | regex.VERSION1):
        # If they gave us a regexp object, extract the pattern.
        pattern = getattr(pattern, 'pattern', pattern)

        self._pattern = pattern
        self._gaps = gaps
        self._flags = flags
        self._regexp = None

        # Remove grouping parentheses -- if the regexp contains any
        # grouping parentheses, then the behavior of re.findall and
        # re.split will change.
        nongrouping_pattern = convert_regexp_to_nongrouping(pattern)

        try:
            self._regexp = regex.compile(nongrouping_pattern, flags)
        except regex.error, e:
            raise ValueError('Error in regular expression {}: {}'.format(pattern, e))

    def tokenize(self, text):
        # If our regexp matches gaps, use re.split:
        if self._gaps:
            return [tok for tok in self._regexp.split(text) if tok]

        # If our regexp matches tokens, use re.findall:
        else:
            return self._regexp.findall(text)

    def span_tokenize(self, text):
        if self._gaps:
            for left, right in regexp_span_tokenize(text, self._regexp):
                if not left == right:
                    yield left, right
        else:
            for m in regex.finditer(self._regexp, text):
                yield m.span()

    def __repr__(self):
        return ('{}(pattern={}, gaps={}, flags={})'.format(
                self.__class__.__name__, self._pattern, self._gaps, self._flags))


class ParagraphTokenizer(NewRegexpTokenizer):
    """
    Tokenize a string into paragraphs.

    This is accomplished by treating any sentence break character plus any text (ie not a space) followed by a new line
    character as the end of a paragraph.

    """
    def __init__(self):
        NewRegexpTokenizer.__init__(self,
                                    ur'(?<=[\u002E\u2024\uFE52\uFF0E\u0021\u003F][\S]*)\s*\n+',
                                    gaps=True)


class WordTokenizer(NewRegexpTokenizer):
    """
    Tokenize a string into words.

    """
    # Match all word contractions, except possessives which we split to retain the root owner.
    CONTRACTION = "([A-Za-z]+\'[A-RT-Za-rt-z]+)"

    # Email pattern, lifted from http://www.regular-expressions.info/email.html
    EMAIL = "(\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\\b)"

    # Capture multi-term names (optionally with 'of' as the second term).
    # We exclude [The, But] from the beggining of multi-term names.
    NAME_COMPOUND = u"((?!(The|But))([A-Z][a-z]+\.?)([^\S\n]of)?([^\S\n][A-Z]+[A-Za-z]+)+)"

    # Capture decimal numbers with allowable punctuation that would get split up with the word pattern
    NUM = u"(\d+(?:[\.\,]{1}\d+)+)"

    # Basic word pattern, strips all punctuation besides special leading characters
    WORD = u"((?:[#@]?)\w+)"

    def __init__(self, detect_compound_names=True):
        pattern = self.EMAIL + '|' +  self.NUM + '|' + self.CONTRACTION + '|' + self.WORD
        if detect_compound_names:
            pattern = self.NAME_COMPOUND + '|' + pattern

        NewRegexpTokenizer.__init__(self, pattern, gaps=False)


class TokenFilter(object):
    """
    Base class for Token Filters.

    All Token Filters must implement a filter method.

    """
    def filter(self, tokens):
        raise NotImplementedError()


class StopwordTokenFilter(TokenFilter):
    """
    Remove stopwords and words with too few characters.

    """
    def __init__(self, stopwords, min_word_size):
        self.stopwords = stopwords
        self.min_word_size = min_word_size

    def filter(self, tokens):
        """
        Return a filtered list of tokens.

        """
        logger.debug('Applying stopword filter')
        return filter(lambda t: t.lower() not in self.stopwords and len(t) > self.min_word_size, tokens)


def regexp_span_tokenize(s, regexp):
    r"""

    Direct copy from NLTK, except for replacing reference of re module with new regex module.


    Return the offsets of the tokens in *s*, as a sequence of ``(start, end)``
    tuples, by splitting the string at each successive match of *regexp*.

        >>> from nltk.tokenize import WhitespaceTokenizer
        >>> s = '''Good muffins cost $3.88\nin New York.  Please buy me
        ... two of them.\n\nThanks.'''
        >>> list(WhitespaceTokenizer().span_tokenize(s))
        [(0, 4), (5, 12), (13, 17), (18, 23), (24, 26), (27, 30), (31, 36),
        (38, 44), (45, 48), (49, 51), (52, 55), (56, 58), (59, 64), (66, 73)]

    :param s: the string to be tokenized
    :type s: str
    :param regexp: regular expression that matches token separators
    :type regexp: str
    :rtype: iter(tuple(int, int))
    """
    left = 0
    for m in regex.finditer(regexp, s):
        right, next = m.span()
        if right != 0:
            yield left, right
        left = next
    yield left, len(s)