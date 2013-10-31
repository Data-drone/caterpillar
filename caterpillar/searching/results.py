# caterpillar: Classes to store search results
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>


class SearchHit(object):
    """
    Represents a single frame that matched the search.

    """
    def __init__(self, frame_id, frame):
        self.frame_id = frame_id
        self.score = 1
        self.frame_terms = set(frame['_positions'].keys())
        self.text = frame['_text']


class SearchResults(list):
    """
    Encapsulates ``SearchHit`` objects in a list. Also provides some extra attributes.

    """
    def __init__(self, query, hits, num_matches, term_weights):
        list.__init__(self, hits)
        self.query = query
        self.hits = hits
        self.num_matches = num_matches
        self.term_weights = term_weights
