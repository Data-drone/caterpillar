# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
import os
import re

MIN_WORD_SIZE = 3  # length of the smallest possible word
APOSTROPHES = u'\u0027\u2019\u02BC\u02BB\u055A\uA78B\uA78C\uFF07'
APOS_RE = re.compile(u'[%s]' % APOSTROPHES)


def parse_stopwords(stopwords_file):
    """
    Parse stopwords from a plain text file.

    Expects a single stopword on every line.

    If a stopword has some flavour of an apostrophe character, we make sure we insert a version of that stopword with
    every type of unicode apostrophe character.

    """
    stopwords = []
    for line in stopwords_file:
        stopword = line.strip()
        if APOS_RE.search(stopword) is not None:
            for apos in APOSTROPHES:
                stopwords.append(APOS_RE.sub(apos, stopword))
        else:
            stopwords.append(stopword)

    return stopwords


# Stopword lists
ENGLISH = None
with open(os.path.join(os.path.dirname(__file__), '../../', 'resources', 'stopwords-english.txt')) as stopwords_file:
    ENGLISH = parse_stopwords(stopwords_file)
ENGLISH_TEST = None
with open(os.path.join(os.path.dirname(__file__), '../../', 'resources',
                       'stopwords-english-test.txt')) as stopwords_file:
    ENGLISH_TEST = parse_stopwords(stopwords_file)
