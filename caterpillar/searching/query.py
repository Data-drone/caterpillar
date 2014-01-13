# caterpillar: Tools to parse and evaluate search queries
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
"""
Query Syntax

Boolean operators:
    dog AND cat -- Text frames that contain both dog and cat.
    dog NOT cat -- Text frames that contain dog but not cat.
    dog OR cat -- Text frames that contain dog or cat.

Wildcards:
    d?g -- '?' represents a single character wildcard.
    d* -- '*' represents a multiple character wildcard.

Term Weighting:
    dog^2 -- Increase the importance of the term dog with a 2x multiplier.

"""
import regex

from lrparsing import Grammar, Keyword, Prio, Ref, Repeat, Token, Tokens

from caterpillar.analytics.influence import InfluenceTopicsPlugin
from caterpillar.processing import schema


class QueryError(Exception):
    """Invalid query"""


class QueryEvaluator(object):
    """
    This class evaluates queries against a term position index supplied at
    instantiation.

    Required Arguments:
    positions_index -- index of term positions.

    """
    def __init__(self, index):
        self.index = index
        self.schema = index.get_schema()
        self.metadata = index.get_metadata()
        self.terms = index.get_frequencies().keys()

    def __call__(self, node):
        """
        This method is called in a bottom-up fashion for each node in the
        parse tree.

        In it, we record a list of ids for frames that match the query
        specified by the current node. When the last (root) node is
        evaluated, we have arrived at the final list of frame ids that match
        the entire query.

        ``node`` is a tuple that holds operator and operand components for
        evaluating a node in the query tree. It is constructred according
        to the ``_QueryGrammar`` definition and contains extra attributes
        specified by the ``_QueryNode`` class.

        """
        node = _QueryNode(node)
        eval_method_name = '_evaluate_' + node[0].name
        if eval_method_name in self.__class__.__dict__:
            # This type of node has an evaluation method defined
            self.__class__.__dict__[eval_method_name](self, node)
        elif type(node[1]) != str and type(node[1]) != unicode:
            # All non-leaf nodes that are not specially evaluated are
            # containers that should inherit state data from children.
            child_node = node[1] if node[0].name != 'brackets' else node[2]
            node.term_weights.update(child_node.term_weights)
            node.frame_ids = child_node.frame_ids
            node.matched_terms = child_node.matched_terms

        return node

    def evaluate(self, query):
        """
        Evaluate a query, returning an instance of ``QueryResult``.

        Required Arguments:
        query -- Query string

        """
        query_tree = _QueryGrammar.parse(query, tree_factory=self)
        return QueryResult(query_tree.frame_ids, query_tree.matched_terms,
                           query_tree.term_weights)

    def _evaluate_field(self, node):
        """
        Evalute metadata field value test.

        """
        operator = node[2][1]
        field_name = self._extract_term_value(node[1])
        field_value = self._extract_term_value(node[3])

        if '*' in field_name or '?' in field_name:
            raise QueryError("Field name cannot contain wildcards")

        wildcard = False
        if '?' in field_value:
            # Insert regex pattern for single character wildcard
            field_value = field_value.replace('?', r'\w')
            wildcard = True
        if '*' in field_value:
            # Insert regex pattern for multiple character wildcard
            field_value = field_value.replace('*', r'[\w]*')
            wildcard = True

        # Get schema field
        try:
            field = self.schema[field_name]
        except KeyError:
            raise QueryError("Invalid field name '{}'".format(field_name))

        if not field.categorical():
            raise QueryError("Improper use of field searching for non-categorical field '{}'".format(field_name))

        if not field.indexed():
            raise QueryError("Non-indexed field '{}' cannot be searched".format(field_name))

        # Determine matching frames
        #
        try:
            frames_by_value = self.metadata[field_name]
        except KeyError:
            # We can get here if no frames have actually stored metadata for the schema field.
            # If this happens then nothing to do here.
            return
        try:
            if operator == '=':
                # Simple equality operator
                if wildcard:
                    # Wildcard equality must compare with all possible field values
                    wildcard_expr = '^' + field_value + '$'
                    for value in frames_by_value:
                        if field.equals_wildcard(value, wildcard_expr):
                            node.frame_ids.update(frames_by_value[value])
                else:
                    # Direct equality is an easy, direct lookup on metadata index
                    field_value = str(field.value_of(field_value))  # Run value through analyser first
                    try:
                        node.frame_ids = set(frames_by_value[field_value])
                    except KeyError:
                        pass
            else:
                if wildcard:
                    raise QueryError("Wildcards are only permitted for field searching when using the '=' operator.")
                # Look for values that match the field comparison expression
                for value in frames_by_value:
                    if field.evaluate_op(operator, value, field_value):
                        # Found a match, add the frames
                        node.frame_ids.update(set(frames_by_value[value]))
        except (NotImplementedError, ValueError) as e:
            raise QueryError(e)

    def _evaluate_operand(self, node):
        """
        Evaluate operand (term) node via lookup in the term positions index.

        """
        term = self._extract_term_value(node)
        wildcard = False
        if term == '*':
            # Single wildcard matches all frames
            node.frame_ids = set(self.index.get_frame_ids())
            return
        if '?' in term:
            # Insert regex pattern for single character wildcard
            term = term.replace('?', r'\w')
            wildcard = True
        if '*' in term:
            # Insert regex pattern for multiple character wildcard
            term = term.replace('*', r'[\w]*')
            wildcard = True
        if wildcard:
            re = regex.compile('^' + term + '$')
            for term in self.terms:
                # Search for terms that match wildcard query
                if re.match(term):
                    node.frame_ids.update(set(self.index.get_term_positions(term).keys()))
                    node.matched_terms.add(term)
        else:
            try:
                node.frame_ids.update(set(self.index.get_term_positions(term).keys()))
                node.matched_terms.add(term)
            except KeyError:
                # Term not matched in index
                pass

    def _evaluate_weighting(self, node):
        """
        Evaluate weighting nodes that can apply to term nodes.

        """
        node.frame_ids = node[1].frame_ids
        node.matched_terms = node[1].matched_terms
        term = self._extract_term_value(node[1])
        weighting_expr = node[2][1]
        weighting_factor = weighting_expr.split('^')[1]
        # Store weighting
        node.term_weights[term] = float(weighting_factor)

    def _evaluate_and_op(self, node):
        """
        Evaluate AND operator node via intersection of operand nodes.

        """
        node.term_weights = dict(node[1].term_weights, **node[3].term_weights)
        node.frame_ids = node[1].frame_ids.intersection(node[3].frame_ids)
        node.matched_terms = node[1].matched_terms.union(node[3].matched_terms)

    def _evaluate_not_op(self, node):
        """
        Evaluate NOT operator node via difference of operand nodes.

        """
        node.frame_ids = node[1].frame_ids.difference(node[3].frame_ids)
        node.matched_terms = node[1].matched_terms

    def _evaluate_or_op(self, node):
        """
        Evaluate OR operator node via union of operand nodes.

        """
        node.term_weights = dict(node[1].term_weights, **node[3].term_weights)
        node.frame_ids = node[1].frame_ids.union(node[3].frame_ids)
        node.matched_terms = node[1].matched_terms.union(node[3].matched_terms)

    def _evaluate_topic(self, node):
        """
        Evaluate topic query.

        """
        topic_name = ' '.join([n[1][1] for n in node[3:]])
        topics_plugin = InfluenceTopicsPlugin(self.index)
        node.frame_ids = topics_plugin.get_topic_frames(topic_name)
        topic_details = topics_plugin.get_topic_details(topic_name)
        node.term_weights = {}
        for term_value in topic_details['primary_terms']:
            # Primary terms receive a 1.5x ranking multiplier
            node.term_weights[term_value] = 1.5
        for term_value in topic_details['secondary_terms']:
            node.term_weights[term_value] = 1

    def _extract_term_value(self, node):
        """
        Extract term value from an operand node; handles arbitrary number of term components (compounds).

        """
        return ' '.join([n[1][1] for n in node[1:]])


class QueryResult(object):
    """
    Encapsulates result data for a single query.

    Fields:
    frame_ids -- A list of IDs for frames that match the query.
    matched_terms -- A dict of matched query terms to their weightings.

    """
    def __init__(self, frame_ids, matched_terms, term_weights):
        self.frame_ids = frame_ids
        self.matched_terms = {mt: 1 for mt in matched_terms}
        for term, weight in term_weights.items():
            self.matched_terms[term] = weight


class _QueryGrammar(Grammar):
    """
    Specifies an lrparsing grammar to parse a query string.

    """
    # Grammar rules.
    expr = Ref("expr")  # Forward reference
    brackets = Token('(') + expr + Token(')')
    and_op = expr << Keyword('and', case=False) << expr
    not_op = expr << Keyword('not', case=False) << expr
    or_op = expr << Keyword('or', case=False) << expr
    term = Token(re=r'[^\s^()=<>:]+')
    operand = Repeat(term, 1)
    weighting = operand << Token(re=r'\^[0-9]*\.?[0-9]+')
    field = operand + Tokens('= < > <= >=') + operand
    topic = Keyword('topic', case=False) + Token(':') + Repeat(term, 1)
    expr = Prio(
        brackets,
        not_op,
        and_op,
        or_op,
        weighting,
        field,
        topic,
        operand
    )
    START = expr    # Where the grammar must start


class _QueryNode(tuple):
    """
    Wrapper for nodes in query parse tree to allow some custom fields.

    """
    def __init__(self, node):
        self.term_weights = dict()
        self.frame_ids = set()
        self.matched_terms = set()
        super(_QueryNode, self).__init__(node)

    def __new__(cls, n):
        return super(_QueryNode, cls).__new__(cls, n)
