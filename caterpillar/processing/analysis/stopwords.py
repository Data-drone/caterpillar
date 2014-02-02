import os

MIN_WORD_SIZE = 3  # length of the smallest possible word


def parse_stopwords(stopwords_file_path):
    """
    Parse stopwords from a plain text file.

    Expects a single stopword on every line.

    """
    stopwords = []
    with open(stopwords_file_path) as stopwords_file:
        for line in stopwords_file:
            stopwords.append(line.strip())

    return stopwords


# Stopword lists
ENGLISH = parse_stopwords(os.path.join(os.path.dirname(__file__), '../../', 'resources', 'stopwords-english.txt'))
ENGLISH_TEST = parse_stopwords(os.path.join(os.path.dirname(__file__), '../../', 'resources',
                                            'stopwords-english-test.txt'))
