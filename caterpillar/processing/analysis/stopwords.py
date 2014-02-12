import os

MIN_WORD_SIZE = 3  # length of the smallest possible word


def parse_stopwords(stopwords_file):
    """
    Parse stopwords from a plain text file.

    Expects a single stopword on every line.

    """
    stopwords = []
    for line in stopwords_file:
        stopwords.append(line.strip())

    return stopwords


# Stopword lists
ENGLISH = None
with open(os.path.join(os.path.dirname(__file__), '../../', 'resources', 'stopwords-english.txt')) as stopwords_file:
    ENGLISH = parse_stopwords(stopwords_file)
ENGLISH_TEST = None
with open(os.path.join(os.path.dirname(__file__), '../../', 'resources',
                       'stopwords-english-test.txt')) as stopwords_file:
    ENGLISH_TEST = parse_stopwords(stopwords_file)
