# caterpillar: Tools to create and store a text index
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@mammothlabs.com.au>
from __future__ import division
import logging

import nltk
import numpy

from caterpillar.processing.tokenize import StopwordTokenFilter, WordTokenizer
from caterpillar.processing import stopwords


logger = logging.getLogger(__name__)


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


def build_text_index(frames, filters=[StopwordTokenFilter(stopwords.ENGLISH, stopwords.MIN_WORD_SIZE)], detect_compound_names=True, frame_ngrams={}):
    """
    This function constructs and returns a ``TextIndex`` object based on frame data.

    Required arguments:
    frames -- A list of ``Frame`` objects for textual data
    filters -- A list of ``TokenFilter`` objects to apply to each frame (defaults to simple English stopword removal)
    detect_compound_names -- This flag indicates whether to identify multi-term names within the text
    frame_ngrams -- An index of bigrams to identify for each frame

    """
    index = TextIndex()
    word_tokenizer = WordTokenizer(detect_compound_names=detect_compound_names)
    total_ngrams_inserted = 0
    logger.info('Performing index')

    # Process frames
    for frame in frames:
        # Tokenize words in frame
        positions = list(word_tokenizer.span_tokenize(frame.text))
        words = [frame.text[l:r] for l,r in positions]

        # Ngram Identification
        if frame.sequence in frame_ngrams.keys():
            num_words = len(words)
            for ngram in frame_ngrams[frame.sequence]:
                matches = []
                ngram_size = len(ngram)

                # Find ngram matches
                for word_cursor in xrange(num_words - ngram_size + 1):
                    # Check ngram by iterating through each term and comparing to words following word cursor
                    for ngram_cursor in xrange(ngram_size):
                        if words[word_cursor+ngram_cursor] <> ngram[ngram_cursor]:
                            # No ngram match on this word cursor
                            break
                    else:
                        # Didn't break loop; this means an ngram was matched
                        matches.append(word_cursor)
                        # Update cursor position, Do NOT allow for overlapping ngrams.
                        word_cursor += ngram_size

                num_insertions = 0
                # Update tokenisation results with ngrams
                for ngram_start_index in matches:
                    ngram_start_index -= num_insertions * ngram_size  # Adjust position for replaced matches
                    ngram_end_index = ngram_start_index + ngram_size - 1
                    # Get character-based positions
                    start_char_pos = positions[ngram_start_index][0]
                    end_char_pos = positions[ngram_end_index][1]
                    # Delete existing records for ngram as separate terms
                    del positions[ngram_start_index:ngram_end_index+1]
                    del words[ngram_start_index:ngram_end_index+1]
                    # Insert new records
                    positions.insert(ngram_start_index, (start_char_pos, end_char_pos))
                    words.insert(ngram_start_index, ' '.join(ngram))
                    num_insertions += 1
                total_ngrams_inserted += num_insertions
                logger.debug("{} n-grams merged in frame {}".format(num_insertions, frame.id))

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
    logger.info("Indexed {} frames, inserted {} n-grams".format(len(index.frames), total_ngrams_inserted))

    return index


def find_bigram_words(frames, stopwords=stopwords.ENGLISH, min_word_size=stopwords.MIN_WORD_SIZE, min_bigram_freq=3, min_bigram_coverage=0.65):
    """
    This function finds bigram words from the specified frames. Names are not considered for bigrams.

    Required arguments:
    frames -- A list of ``Frame`` objects for textual data
    stopwords -- Stopword list to exclude from bigram binding
    min_word_size -- The length cutoff for words to be considered for bigrams
    min_bigram_freq -- Minimum frequency cutoff for each bigram
    min_bigram_coverage -- Cutoff ratio for bigram frequency over individual word frequencies

    Returns a tuple consisting of two elements:
        - a list of bigrams,
        - a dictionary of frame sequence number to bigram list

    """
    word_tokenizer = WordTokenizer()
    frequencies = nltk.probability.FreqDist()
    logger.info("Identifying n-grams")

    # Convert to dict for faster access
    stopwords = {s : None for s in stopwords}

    # Generate a table of candidate bigrams
    candidate_bigrams = {}
    for frame in frames:
        prev_word = None
        for word in word_tokenizer.tokenize(frame.text):
            if word[0].isupper():
                # Skip names
                prev_word = None
                continue
            if word in stopwords or len(word) < min_word_size:
                # Skip stopwords
                prev_word = None
                continue
            if prev_word:
                # Record bigram
                bigram = (prev_word, word)
                try:
                    candidate_bigrams[bigram].add(frame.sequence)
                except KeyError:
                    candidate_bigrams[bigram] = set([frame.sequence])
            prev_word = word
            frequencies.inc(word)

    # Filter and sort by frequency-decreasing
    vals = [len(v) for v in candidate_bigrams.values()]
    mean = numpy.mean(vals)
    std = numpy.std(vals)
    candidate_bigrams_list = filter(lambda (k, v): len(v) > min_bigram_freq, candidate_bigrams.items())
    candidate_bigrams_list = filter(lambda (k, v): len(v) / frequencies[k[0]] > min_bigram_coverage and
                                    len(v) / frequencies[k[1]] > min_bigram_coverage, candidate_bigrams_list)
    candidate_bigrams_list = sorted(candidate_bigrams_list, key=lambda (k, v): len(v), reverse=True)

    # Build index of bigrams for frames
    frame_bigrams = {}
    for b in candidate_bigrams_list:
        for frame_seq in b[1]:
            try:
                frame_bigrams[frame_seq].append(b[0])
            except KeyError:
                frame_bigrams[frame_seq] = [b[0]]
    logger.info("{} n-grams identified".format(len(candidate_bigrams_list)))

    return [b[0] for b in candidate_bigrams_list], frame_bigrams
