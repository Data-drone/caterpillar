# Copyright (c) Kapiche Limited
# Author: Sam Hames <sam.hames@kapiche.com>

"""
Operators for creating complex searches by composition of simple searches.

The :class:`IndexReader` exposes :meth:`.filter` and :meth:`.search` which provide fast low level
access to frames and docments in an index by both structured and unstructured data. Not all search
needs are met by this method, however, so the methods in this module allow composition and ranking
of resultsets.

A resultset is a dictionary mapping a frame_id or document_id to a list of scores:
    {1: [10], 211: [4], 102: [8]}

Results from `IndexReader.filter` will have the TF-IDF score of matching terms (or 0 if only
metadata was used to filter) - these primitive queries have only a single score. When combining
resultsets a list of scores for each matching result is accumulated.

This module provides three operators for combining resultsets: match_all for taking the intersection
of multiple resultsets, match_any for taking the union and exclude for removing results from one set
that occur in a different set.

Examples
    >>> with IndexReader('/path/to/index') as r:
    ...    result1 = r.filter(must=[('cat', 'cats', 'dog', 'dogs'), 'pet'], return_documents=False)
    ...    result2 = r.filter(should=['petshop', 'kennel'], return_documents=False)
    ...    result3 = r.filter(metadata={'timestamp': {'>=': '2016-01-01T00:00:00'}}, return_documents=False)
    ...
    ...    # Intersection of all result sets
    ...    result4 = match_all(result1, result2, result3)
    ...
    ...    # Union of result sets
    ...    result5 = match_any(result1, result2)
    ...
    ...    # Remove anything matching result3 from result5
    ...    result6 = exclude(result5, result3)
    ...
    ...    # Score and rank result5, taking the maximum of each score from the low level matches and
    ...    # returning only the first 10 keys
    ...    scored = score_and_rank(result5, aggregator=max, start=0, limit=10):
    ...
    ...    # Score and rank result6, taking the sum of all the individual scores from each match.
    ...    scored2 = score_and_rank(result6, aggregator=sum)
    ...
    ...    # Load the frames corresponding to scored:
    ...    frames = list(r.get_frames(None, frame_ids=[i[0] for i in scored2]))
    ...

"""


def match_all(*result_sets):
    """
    Take the intersection of all the result_sets.

    If a key is missing from any of the input sets, it will not be present in the output.

    """
    # View keys is like a set object of the dictionary keys - this will become keys() in Python 3
    keep_keys = result_sets[0].viewkeys()

    for d in result_sets[1:]:
        keep_keys &= d.viewkeys()

    output = {key: [i for d in result_sets for i in d[key]] for key in keep_keys}

    return output


def match_any(*result_sets):
    """
    Take the union of all the result_sets.

    If a key is present in any of the input sets, it will be present in the output. For any key in
    the output set that is missing from a result set, a score of 0 will be assigned.

    """

    # View keys is like a set object of the dictionary keys - this will become keys() in Python 3
    all_keys = result_sets[0].viewkeys()

    for d in result_sets[1:]:
        all_keys |= d.viewkeys()

    # Note that explicit 0 scores are assigned if a key is missing, so that each score set has one
    # entry per query component.
    output = {key: [i for d in result_sets for i in d.get(key, [0])] for key in all_keys}

    return output


def exclude(result_set, *exclusions):
    """Match keys in result_set, but only if they do not occur in any of the exclusions."""
    keep_keys = result_set.viewkeys()

    for exclusion in exclusions:
        keep_keys -= exclusion.viewkeys()

    return {key: result_set[key] for key in keep_keys}


def boost(result_set, boost):
    """Multiply each individual score for this result set by boost."""
    return {key: [i * boost for i in value] for key, value in result_set.items()}


def score_and_rank(result_set, aggregator=sum, start=0, limit=0):
    """
    Aggregate the scores for each match, then return tuples of (id, score) in descending order.

    The aggregator function should take a sequence of numbers and return a single number.

    """
    aggregated = sorted(
        ((object_id, aggregator(value)) for object_id, value in result_set.items()),
        key=lambda x: (x[1], -x[0]),  # Deterministic sort on keys if scores are identical
        reverse=True
    )

    if limit > 0:
        return aggregated[start:start + limit]
    else:
        return aggregated
