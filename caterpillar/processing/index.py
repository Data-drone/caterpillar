# caterpillar: Tools to create and store a text index
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@mammothlabs.com.au>
from __future__ import division
import logging

import nltk
import numpy
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.analyse import DefaultAnalyser, PotentialBiGramAnalyser, BiGramAnalyser
from caterpillar.processing.analysis.tokenize import WordTokenizer


logger = logging.getLogger(__name__)


class Index(object):
    """
    An index of all the text within a data set; holds statistical information to be used in retrieval and analytics. The
    main data structures are:

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

    **frames** gives all frames by id::

        {
            frame_id: {...frame_data...}
        }

    """
    def __init__(self):
        self.term_positions = {}
        self.term_associations = {}
        self.frames = {}
        self.term_frequencies_by_frame = nltk.probability.FreqDist()

    def inc_term_frequency(self, term, amount=1):
        """
        Increment the frequency count of a term.

        Required Arguments
        term -- The term string we are incrementing the count for.

        Optional Arguments
        amount -- How much to increment by as an int.

        """
        self.term_frequencies_by_frame.inc(term, amount)

    def add_frame(self, frame):
        """
        Add a frame to the index.

        This method copies the values of the frame object into a dict then stores it by frame id.

        Required Arguments
        frame -- The Frame object to add.
        """
        self.frames[frame.id] = {
            'sequence': frame.sequence,
            'text': " ".join(frame.sentences),  # Join together the sentences
            'metadata': frame.metadata.copy() if frame.metadata else dict(),
            'unique_terms': frame.unique_terms,
        }

    def fold_term_case(self, merge_threshold=0.7):
        """
        Perform case folding on the index, merging words into names and vice-versa depending on the specified threshold.

        Optional Arguments:
        merge_threshold -- A float used to test when to merge two variants. When the ratio between word and name version
        of term falls below this threshold the merge is carried out.

        """
        term_transforms = []
        for w, freq in self.term_frequencies_by_frame.items():
            if w.islower() and w.title() in self.term_frequencies_by_frame:
                freq_name = self.term_frequencies_by_frame[w.title()]
                if freq / freq_name < merge_threshold:
                    # Merge into name
                    term_transforms.append((w, w.title()))
                elif freq_name / freq < merge_threshold:
                    # Merge into word
                    term_transforms.append((w.title(), w))
        self._update_for_term_transofrms(term_transforms)

    def _update_for_term_transofrms(self, transforms):
        """
        Update the index based on a list of term transformations.

        Required Arguments
        transforms -- A list of tuples represent transformations. Each transformation is encoded in a tuple like:
        (old_term, new_term)

        """
        for old_term, new_term in transforms:

            # Update term positions in frames
            for frame_id, positions in self.term_positions[old_term].items():
                try:
                    self.term_positions[new_term][frame_id].extend(positions)
                except KeyError:
                    # New term had not been recorded in this frame
                    self.term_positions[new_term][frame_id] = positions
                # Update frame words
                self.frames[frame_id]['unique_terms'].add(new_term)
                self.frames[frame_id]['unique_terms'].remove(old_term)
            del self.term_positions[old_term]

            # Update term associations
            if old_term in self.term_associations:
                # All terms will have an empty dict as their associations as a minimum so no need to check.
                for other_term, freq in self.term_associations[old_term].items():
                    # Update new term
                    try:
                        self.term_associations[new_term][other_term] += freq
                    except KeyError:
                        # New term had no recorded associations with other term
                        self.term_associations[new_term][other_term] = freq
                    # Update other term
                    try:
                        self.term_associations[other_term][new_term] += self.term_associations[other_term][old_term]
                    except KeyError:
                        # Other term had no recorded associations with new term
                        self.term_associations[other_term][new_term] = self.term_associations[other_term][old_term]
                    del self.term_associations[other_term][old_term]
                del self.term_associations[old_term]

            # Update term frequency
            self.term_frequencies_by_frame[new_term] += self.term_frequencies_by_frame[old_term]
            del self.term_frequencies_by_frame[old_term]


def build_text_index(frames, analyser=DefaultAnalyser(), fold_case=True):
    """
    This function constructs and returns an ``Index`` object constructed from the passed frame iterator.

    Required arguments:
    frames -- An iterable of ``Frame`` objects for textual data

    Optional Arguments:
    analyser -- An ``Analyser`` to user for token extraction. Defaults to ``DefaultAnalyser``.
    fold_case -- This bool flag indicates whether to call ``Index.fold_term_case`` on the index to be returned. Defaults
    to True.

    Returns a new ``Index`` object.

    """
    index = Index()
    logger.info('Performing index')

    # Process frames
    for frame in frames:
        # We need to tokenize each of the sentences in the frame.
        terms_seen = set()
        for sentence in frame.sentences:
            # Tokenize words in sentence
            tokens = analyser.analyse(sentence)

            # Record positional information
            for token in tokens:
                # Add to the list of terms we have seen if it isn't already there.
                if not token.stopped:
                    # Add it to the terms seen
                    terms_seen.add(token.value)

                    # Record word positions on text index
                    try:
                        index.term_positions[token.value][frame.id].append(token.index)
                    except KeyError:
                        # Handle if frame doesn't exist
                        try:
                            index.term_positions[token.value][frame.id] = [token.index]
                        except KeyError:
                            # Handle if term not entered yet
                            index.term_positions[token.value] = {frame.id: [token.index]}
                            index.term_associations[token.value] = {}

        # Record co-occurrences and frequencies for the terms we have seen across all sentences in the frame
        for term in terms_seen:
            index.inc_term_frequency(term)  # Record frequency information
            for other_term in terms_seen:
                if term == other_term:
                    # same term
                    continue
                # Record word associations on text index
                index.term_associations[term][other_term] = index.term_associations[term].get(other_term, 0) + 1

        # Store unique terms and save the frame
        frame.unique_terms = terms_seen
        index.add_frame(frame)

    # Perform case folding on the index if we need to
    if fold_case:
        index.fold_term_case()

    logger.info("Indexed {} frames".format(len(index.frames)))

    return index


def build_text_index_with_bi_grams(frames, bi_grams, case_folding=True):
    """
    A shortcut function to build an ``Index`` object from an interable of ``Frame``s using a ``BiGramAnalyser``.

    Required Arguments
    frames -- An iterable of ``Frame`` objects for textual data
    bi_grams -- A list of string bi-grams to match. Passed onto the ``BiGramAnalyser``.

    Returns a new ``Index`` object.

    """
    return build_text_index(frames, analyser=BiGramAnalyser(bi_grams), fold_case=case_folding)


def find_bi_gram_words(frames, min_bi_gram_freq=3, min_bi_gram_coverage=0.65):
    """
    This function finds bi-gram words from the specified ``Frame``s iterable which pass certain criteria.

    This function uses a ``PotentialBiGramAnalyser`` to identify potential bi-grams. Names and stopwords are not
    considered for bi-grams. Only bi-grams that have a certain frequency and coverage are considered.

    Required Arguments:
    frames -- An iterable of ``Frame`` objects for textual data

    Optional Arguments:
    min_bi_gram_freq -- A int representing the minimum frequency cutoff for each bi_gram
    min_bi_gram_coverage -- A float representing the cutoff ratio for bi_gram frequency over individual word frequencies

    Returns a list of bi-gram strings that pass the criteria.

    """
    logger.info("Identifying n-grams")

    # Generate a table of candidate bigrams
    candidate_bi_grams = nltk.probability.FreqDist()
    uni_gram_frequencies = nltk.probability.FreqDist()
    bi_gram_analyser = PotentialBiGramAnalyser()
    for frame in frames:
        for sentence in frame.sentences:
            terms_seen = set()
            for token_list in bi_gram_analyser.analyse(sentence):
                # Using a special filter that returns list of tokens. List of 1 means no bi-grams.
                if len(token_list) > 1:  # We have a bi-gram people!
                    candidate_bi_grams.inc("{} {}".format(token_list[0].value, token_list[1].value))
                for t in token_list:  # Keep a list of terms we have seen so we can record freqs later.
                    if not t.stopped:  # Naughty stopwords!
                        terms_seen.add(t.value)
            for term in terms_seen:
                uni_gram_frequencies.inc(term)

    # Filter and sort by frequency-decreasing
    candidate_bi_gram_list = filter(lambda (k, v): v > min_bi_gram_freq, candidate_bi_grams.items())
    candidate_bi_gram_list = filter(lambda (k, v): v / uni_gram_frequencies[k.split(" ")[0]] > min_bi_gram_coverage
                                    and v / uni_gram_frequencies[k.split(" ")[1]] > min_bi_gram_coverage,
                                    candidate_bi_gram_list)

    logger.info("{} n-grams identified".format(len(candidate_bi_gram_list)))

    return [b[0] for b in candidate_bi_gram_list]
