# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Tools to tokenize text."""
from nltk.internals import convert_regexp_to_nongrouping
import regex


class Token(object):
    """
    A class representing a "token" (usually a word) extracted from the source text being analysed.

    Because object instantiation in Python is slow all tokenizers use this class as singleton. This means that ONE
    SINGLE Token object is YIELD OVER AND OVER by tokenizers, changing the attributes each time.

    This trick means that consumers of tokens (i.e. filters) must never try to hold onto the token object between loop
    iterations, or convert the token generator into a list. Instead, save the attributes between iterations,
    not the object!

    """
    def __init__(self, value=None, position=None, stopped=None, index=None):
        self.value = value
        self.position = position
        self.stopped = stopped
        self.index = index

    def update(self, value, stopped=False, position=None, index=None):
        """
        Re-initialise this token instance with the passed values.

        Required Arguments
        value -- The unicode value of this Token.

        Optional Arguments
        stopped -- A bool indicating if this token was stopped by a filter.
        position --  A int indicating the original position in the stream. This is a 0 based index.
        index --  A tuple of two ints indicating the start and end index of this token in the original stream.

        Returns this token.

        """
        self.value = value
        self.position = position
        self.stopped = stopped
        self.index = index
        return self

    def copy(self):
        """
        Return a deep copy of this object.

        """
        token = Token()
        token.update(self.value, self.stopped, self.position, self.index)
        return token


class Tokenizer(object):
    """
    Abstract base class for all Tokenizers.

    Forces all implementers to implement a tokenize() method.

    """
    def tokenize(self, *args, **kwargs):
        raise NotImplementedError


class RegexpTokenizer(Tokenizer):
    """
    A ``Tokenizer`` that splits a string using a regular expression.

    This class can be used to match either the tokens or the separators between tokens.

        >>> tokenizer = RegexpTokenizer('\w+|\$[\d\.]+|\S+')

    This class uses the new python regex module instead of the existing re module. This means unicode codepoint
    properties are supported.

        >>> tokenizer = RegexpTokenizer('\w+|\$[\p{N}\.]+|\S+')

    Required Arguments
    pattern -- A str used to build this tokenizer. This pattern may safely contain grouping parentheses.

    Optional Arguments
    gaps -- A bool indicating this tokenizer's pattern should be used to find separators between tokens; False if this
        tokenizer's pattern should be used to find the tokens themselves. Defaults to False.
    flags -- A int mask of regexp flags used to compile this tokenizer's pattern.  By default, the following flags are
        used: `regex.UNICODE | regex.MULTILINE | regex.DOTALL | regex.VERSION1`.

    """
    def __init__(self, pattern, gaps=False, flags=regex.UNICODE | regex.MULTILINE | regex.DOTALL):
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

    def tokenize(self, value):
        """
        Perform the tokenizing.

        Required Argument
        value -- The unicode string to tokenize.

        """
        t = Token()  # The token instance we will reuse
        if not self._gaps:
            # The default: expression matches are used as tokens
            for pos, match in enumerate(self._regexp.finditer(value)):
                yield t.update(match.group(0), index=(match.start(), match.end(),), position=pos)
        else:
            # When gaps=True, iterate through the matches and
            # yield the text between them.
            left = 0
            last_pos = 0
            for pos, match in enumerate(regex.finditer(self._regexp, value)):
                right, next = match.span()
                if right != 0:
                    yield t.update(value[left:right], position=pos, index=(left, right,))
                left = next
                last_pos = pos
            if left != len(value):
                yield t.update(value[left:], position=last_pos+1, index=(left, len(value),))


class ParagraphTokenizer(RegexpTokenizer):
    """
    Tokenize a string into paragraphs.

    This is accomplished by treating any sentence break character plus any text (ie not a space) followed by a new line
    character as the end of a paragraph.

    Because of titles etc., we also treat any two or more consecutive newline characters as a paragraph break.

    """
    def __init__(self):
        RegexpTokenizer.__init__(
            self, ur'(?<=[\u002E\u2024\uFE52\uFF0E\u0021\u003F][\S]*)\s*(?:\r?\n)+|(?:\r?\n){2,}', gaps=True
        )


class WordTokenizer(RegexpTokenizer):
    """
    Tokenize a string into words.

    This ``Tokenizer`` contains a bunch of special logic that in an ideal world would be separated out into filters.
    Unfortunately this isn't an ideal world and filters can be slow. The tokenizer will:
      * return compound names as a single token;
      * keep emails in-tact as a single token;
      * keep contractions except possessives as a single token;
      * return numbers, including decimal numbers as a single token; and
      * break the text into words based on whitespace separation and punctuation (excepted where stated);

    """
    # Match all word contractions, except possessives which we split to retain the root owner.
    # We discard possessives because the are of no use (in english anyway).
    # TODO: This won't work for languages like French!!
    CONTRACTION = "([A-Za-z]+\'[A-RT-Za-rt-z]+)"

    # Email pattern, lifted from http://www.regular-expressions.info/email.html
    EMAIL = "(\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\\b)"

    # Capture multi-term names (optionally with 'of' as the second term).
    # We exclude [The, But] from the beggining of multi-term names.
    NAME_COMPOUND = u"((?!(The|But))([A-Z][a-z]+|[A-Z][a-z]{0,2}\.)([^\S\n]of)?([^\S\n][A-Z]+[A-Za-z]+)+)"

    # Capture decimal numbers with allowable punctuation that would get split up with the word pattern
    NUM = u"(\d+(?:[\.\,]{1}\d+)+)"

    # Basic word pattern, strips all punctuation besides special leading characters
    WORD = u"([#@]?\w+)"

    # URL pattern (Based on http://stackoverflow.com/questions/833469/regular-expression-for-url)
    URL = u"((((mailto:|ftp|http(s?)){1}:(?:\/\/)?)" \
          + "(?:[-;:&=\+\$,\w]+@)?[A-Za-z0-9.-]+|(?:www\.|[-;:&=\+\$,\w]+@)[A-Za-z0-9.-]+)" \
          + "((?:\/[\+~%\/.\w-_]*)?\??(?:[-\+=&;%@.\w_]*)#?(?:[\w]*))?)"

    def __init__(self, detect_compound_names=True):
        pattern = self.URL + '|' + self.EMAIL + '|' + self.NUM + '|' + self.CONTRACTION + '|' + self.WORD
        if detect_compound_names:
            pattern = self.NAME_COMPOUND + '|' + pattern

        RegexpTokenizer.__init__(self, pattern, gaps=False)


class SimpleWordTokenizer(RegexpTokenizer):
    """
    Tokenize a string into words using a set of very simple rules.

    This :class:`RegexpTokenizer` contains very little special logic. It will:
      * split token on whitespace;
      * return compound names as a single token;

    """
    # Capture multi-term names (optionally with 'of' as the second term).
    # We exclude [The, But] from the beginning of multi-term names.
    NAME_COMPOUND = u"((?!(The|But))([A-Z][a-z]+|[A-Z][a-z]{0,2}\.)([^\S\n]of)?([^\S\n][A-Z]+[A-Za-z]+)+)"
    WORD = u"\S+"

    def __init__(self, detect_compound_names=True):
        pattern = self.WORD
        if detect_compound_names:
            pattern = self.NAME_COMPOUND + '|' + pattern

        RegexpTokenizer.__init__(self, pattern, gaps=False)


class EverythingTokenizer(Tokenizer):
    """
    Returns entire input string as a single token generator.

    """
    def tokenize(self, value):
        t = Token()
        yield t.update(value, stopped=False, position=0, index=(0, len(str(value)) if value else 0,))
