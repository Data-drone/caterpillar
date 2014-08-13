# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""

"""
import re
import nltk


class Preprocessor(object):
    """Base class for a preprocessor."""
    def __call__(self, value):
        return self.preprocess(value)

    def preprocess(self, value):
        raise NotImplementedError()


class RePreprocessor(Preprocessor):
    """Convenience class for running a :func:`re.sub` over a value and returning it."""
    def __init__(self, pattern, repl):
        """``pattern`` (str) is the pattern to use with re.sub."""
        self._pattern = pattern
        self._repl = self._repl

    def preprocess(self, value):
        return re.sub(self._pattern, self._repl, value)


class FramePreprocessor(Preprocessor):
    """
    Preprocessor that returns a copy of value with '\x02\x03' inserted where frame boundaries fall.

    .. warning::
        Unfortunately this code is damn ugly and full of hacks. Because we decided not to go to the effort of
        implementing our own punkt tokenizer, we need to get into the internals of the one provided by NLTK to do things
        efficiently.

    """
    def __init__(self, frame_size):
        super(FramePreprocessor, self).__init__()
        self.frame_size = frame_size
        self._sent_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
        # Compile re's once
        tok = self._sent_tokenizer
        self._sent_sub = re.compile(r"(\S*{})(?=(?P<after_tok>(?P<punct_tok>{}+)|\s+\S+))".
                                    format(tok._lang_vars._re_sent_end_chars, tok._lang_vars._re_non_word_chars),
                                    flags=re.DOTALL | re.UNICODE | re.MULTILINE)
        self._para_sub = re.compile(u'\x02\\S{0,4}\\s*(?:\r?\n)+|\r?\n(?:\r?\n)+', flags=re.DOTALL | re.UNICODE | re.MULTILINE)
        self._frame_sub = re.compile(u'(?:[^\x03\x02]*(?:\x02|(?=\x03)|$)){1,2}[\\s\x03]*',
                                     flags=re.DOTALL | re.UNICODE | re.MULTILINE)

    def preprocess(self, value):
        # Mark sentences with \x02
        # Sentences are complicated because of the NLTK punkt tokenizer design
        tok = self._sent_tokenizer
        sentence_breaks = []
        last_break = 0
        for match in self._sent_sub.finditer(value):
            context = match.group() + match.group('after_tok')
            if tok.text_contains_sentbreak(context):
                if match.group('punct_tok'):
                    sentence_breaks.append(slice(last_break, match.end('punct_tok')))
                    last_break = match.end('punct_tok')
                else:
                    sentence_breaks.append(slice(last_break, match.end()))
                    last_break = match.end()
        # Don't forget the end bit!
        sentence_breaks.append(slice(last_break, len(value)))
        value = u'\x02'.join([value[sl] for sl in sentence_breaks])
        # Mark paragraphs with \x03
        value = self._para_sub.sub(u'\\g<0>\x03', value)
        # Mark frames using sentence and paragraph markers with \x04
        value = self._frame_sub.sub(u'\\g<0>\x04', value)
        # Remove sentence and paragraph markers
        return re.sub(u'\x02|\x03', '', value, flags=re.DOTALL | re.UNICODE | re.MULTILINE)
