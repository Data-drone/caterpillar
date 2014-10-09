# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""Classes to store search results."""


class SearchHit(object):
    """
    Represents a single frame that matched the search.

    """
    def __init__(self, frame_id, frame):
        self.frame_id = frame_id
        self.doc_id = frame['_doc_id']
        self.score = 1
        self.frame_terms = set(frame['_positions'].keys())
        # Extract the important data off the frame so it is easily available
        self.data = {k: v for k, v in frame.items() if k == '_field' or k[0] != '_'}  # Don't expose private frame items
        self.data['_id'] = frame_id
        self.data['_doc_id'] = self.doc_id
        if '_text' in frame:
            self.data[frame['_field']] = frame['_text']  # Make the text available at it's original field name
            self.text_field = frame['_field']


class SearchResults(list):
    """
    Encapsulates ``SearchHit`` objects in a list. Also provides some extra attributes.

    """
    def __init__(self, query, hits, num_matches, term_weights):
        list.__init__(self, hits)
        self.query = query
        self.num_matches = num_matches
        self.term_weights = term_weights
