# caterpillar: Tools to create and store a text index
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>

import nltk.corpus

from caterpillar.processing.tokenize import StopwordTokenFilter, WordTokenizer
from caterpillar.processing import stopwords


class TextIndex(object):
    """
    An index of all the text within a data set; holds statistical information to be used in retrieval and analytics.

    **term_positions** gives character positions for each word across all frames in the format::

        {
            term: {
                frame_id: [(start, end), (start, end)]
            }
        }

    **term_associations** gives co-occurrence counts for all word pairs in the format::

        {
            term: {
                other_term: count
            }
        }

    **term_frequencies_by_frame** gives frequency counts at the frame resolution for all words in the format::

        {
            term: count
        }

    **frames** gives raw ``Frame`` objects by frame id

    """
    def __init__(self):
        self.term_positions = {}
        self.term_associations = {}
        self.frames = {}
        self.term_frequencies_by_frame = nltk.probability.FreqDist()

    def inc_term_frequency(self, term, amount=1):
        """
        Increment the frequency count of a term.

        """
        self.term_frequencies_by_frame.inc(term, amount)


def build_text_index(frames, filters=[StopwordTokenFilter(stopwords.ENGLISH, stopwords.MIN_WORD_SIZE)], detect_compound_names=True):
    """
    This function constructs and returns a ``TextIndex`` object based on frame data.

    Required arguments:
    frames -- A list of ``Frame`` objects for textual data
    filters -- A list of ``TokenFilter`` objects to apply to each frame (defaults to simple English stopword removal)

    """
    index = TextIndex()
    word_tokenizer = WordTokenizer(detect_compound_names=detect_compound_names)

    for frame in frames:

        # Tokenize words in frame
        positions = list(word_tokenizer.span_tokenize(frame.text))
        words = [frame.text[l:r] for l,r in positions]
        unique_words = set(words)

        # Apply filters
        for f in filters:
            unique_words = f.filter(unique_words)

        # Store the unique words with the frame
        frame.unique_words = set(unique_words)

        # Store frame object
        index.frames[frame.id] = frame

        # Marshall word positions in this frame
        word_positions = {w: [] for w in unique_words}
        for i in range(0, len(words)):
            try:
                word_positions[words[i]].append(positions[i])
            except KeyError:
                # Ignore stopwords, etc
                pass

        for word in unique_words:

            # Record word positions on text index
            try:
                index.term_positions[word][frame.id] = word_positions[word]
            except KeyError:
                # Handle first frame that a word appears in
                index.term_positions[word] = {frame.id : word_positions[word]}

            # Record frequency information
            index.inc_term_frequency(word)

            # Loop through possible word pairs
            for other_word in unique_words:

                if word == other_word:
                    # same word
                    continue

                # Record word associations on text index
                try:
                    index.term_associations[word][other_word] = index.term_associations[word].get(other_word, 0) + 1
                except KeyError:
                    # Handle first association recorded for a word
                    index.term_associations[word] = {other_word : 1}

    return index

