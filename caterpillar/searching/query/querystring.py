# Copyright (C) Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""
This module supports all basic querying functionality in query string format, which is exposed through the
``QueryStringQuery`` class.

Examples:

Boolean operators:
    telephone AND email -- Text frames that contain both 'telephone' and 'email'.
    telephone NOT email -- Text frames that contain 'telephone' but not 'email'.
    telephone OR email -- Text frames that contain 'telephone' or 'email'.

Wildcards:
    ?mail -- '?' represents a single character wildcard.
    *phon* -- '*' represents a multiple character wildcard.

Term Weighting:
    telephone^2 OR email -- Increase the importance of the term 'telephone' in the query by a 2x multiplier.

Field Equality:
    score=9 -- Text frames which have 'score' metadata  of '9'. (<, <=, >, >= operators supported for numeric fields)

"""
import regex

from lrparsing import Grammar, Keyword, ParseError, Prio, Ref, Repeat, Token, Tokens

from caterpillar.processing import schema
from caterpillar.searching.query import BaseQuery, QueryError, QueryResult


class QueryStringQuery(BaseQuery):
    """
    This class allows term and metadata based querying via raw query string passed to ``query_str``.

    Optionally restricts query to the specified ``text_field``.

    """
    def __init__(self, query_str, text_field=None):
        self.query_str = query_str
        self.text_field = text_field

    def evaluate(self, index):
        frame_ids, term_weights = _QueryStringParser(index).parse_and_evaluate(self.query_str)
        if self.text_field is not None:
            # Restrict for text field
            metadata = index.get_metadata()
            if self.text_field not in metadata:
                raise QueryError("Specified text field {} doesn't exist".format(self.text_field))
            frame_ids.intersection_update(set(metadata[self.text_field]['_text']))

        return QueryResult(frame_ids, term_weights)


class _QueryStringParser(object):
    """
    This class parses and evaluates query strings against a specified ``index``.

    """
    def __init__(self, index):
        self.index = index
        self.schema = index.get_schema()
        self.metadata = index.get_metadata()
        self.terms = index.get_frequencies().keys()

    def __call__(self, node):
        """
        This method is called in a bottom-up fashion for each node in the parse tree.

        In it, we record a list of ids for frames that match the query specified by the current node. When the last
        (root) node is evaluated, we have arrived at the final list of frame ids that match the entire query string.

        ``node`` is a tuple that holds operator and operand components for evaluating a node in the query tree. It is
        constructred according to the ``_QueryStringGrammar`` definition and contains extra attributes specified by the
        ``_QueryStringNode`` class.

        """
        node = _QueryStringNode(node)
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

    def parse_and_evaluate(self, query):
        """
        Evaluate a query string, returning a 2-tuple containing (frame_ids, term_weights).

        The term weights represent specific weighting values for terms specified in the query. They default to 1 unless
        modified explicity using the appropriate query sytax.

        """
        try:
            query_tree = _QueryStringGrammar.parse(query, tree_factory=self)
        except ParseError, e:
            raise QueryError("Invalid query syntax.")

        # Only pass on term weights for terms that were a positive match
        term_weights = {mt: 1 for mt in query_tree.matched_terms}
        for term, weight in query_tree.term_weights.iteritems():
            term_weights[term] = weight

        return (query_tree.frame_ids, term_weights)

    def _evaluate_field(self, node):
        """
        Evalute metadata field value test.

        """
        operator = node[2][1]
        field_name = self._extract_term_value(node[1])
        field_value = self._extract_term_value(node[3])

        if '*' in field_name or '?' in field_name:
            raise QueryError("Field name cannot contain wildcards")

        # Get schema field
        try:
            field = self.schema[field_name]
        except KeyError:
            raise QueryError("Invalid field name '{}'".format(field_name))

        if not field.indexed():
            raise QueryError("Non-indexed field '{}' cannot be searched".format(field_name))

        if not field.categorical():
            raise QueryError("Cannot use field comparison syntax for non-categorical field '{}'".format(field_name))

        # Categorical field
        #
        wildcard = False
        if '?' in field_value:
            # Insert regex pattern for single character wildcard
            field_value = field_value.replace('?', r'\w')
            wildcard = True
        if '*' in field_value:
            # Insert regex pattern for multiple character wildcard
            field_value = field_value.replace('*', r'[\w]*')
            wildcard = True

        # Determine matching frames
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
                    raise QueryError("Wildcards only permitted for field searching when using the '=' operator.")
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
        value = self._extract_term_value(node)
        if value == '*':
            # Single wildcard matches all frames
            node.frame_ids = set(self.index.get_frame_ids())
        else:
            wildcard = False
            if '?' in value:
                # Insert regex pattern for single character wildcard
                value = value.replace('?', r'\w')
                wildcard = True
            if '*' in value:
                # Insert regex pattern for multiple character wildcard
                value = value.replace('*', r'[\w]*')
                wildcard = True
            if wildcard:
                re = regex.compile('^' + value + '$')
                for term in self.terms:
                    # Search for terms that match wildcard query
                    if re.match(term):
                        node.frame_ids.update(set(self.index.get_term_positions(term).keys()))
                        node.matched_terms.add(term)
            else:
                try:
                    node.frame_ids.update(set(self.index.get_term_positions(value).keys()))
                    node.matched_terms.add(value)
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

    def _extract_term_value(self, node):
        """
        Extract term value from an operand node; handles arbitrary number of term components (compounds).

        """
        term_value = ' '.join([n[1][1] for n in node[1:]])
        term_value = term_value.replace('"', '')  # Remove quotes
        return term_value


class _QueryStringGrammar(Grammar):
    """
    Specifies an lrparsing grammar to parse a query string.

    """
    # Grammar rules.
    expr = Ref("expr")  # Forward reference
    brackets = Token('(') + expr + Token(')')
    and_op = expr << Token(re=r'(?i)(?<!=")and(?!")') << expr
    not_op = expr << Token(re=r'(?i)(?<!=")not(?!")') << expr
    or_op = expr << Token(re=r'(?i)(?<!=")or(?!")') << expr
    term = Token(re=r'[^\s^()=<>:]+')
    operand = Repeat(term, 1)
    weighting = operand << Token(re=r'\^[0-9]*\.?[0-9]+')
    field = operand + Tokens('= < > <= >=') + operand
    expr = Prio(
        brackets,
        not_op,
        and_op,
        or_op,
        weighting,
        field,
        operand
    )
    START = expr    # Where the grammar must start


class _QueryStringNode(tuple):
    """
    Wrapper for nodes in query parse tree to allow some custom fields.

    At every stage of evaluating the query tree, an instance of this class will be created to store the evaluation
    results.

    """
    def __init__(self, node):
        self.frame_ids = set()
        self.term_weights = dict()
        self.matched_terms = set()
        super(_QueryStringNode, self).__init__(node)

    def __new__(cls, n):
        return super(_QueryStringNode, cls).__new__(cls, n)
