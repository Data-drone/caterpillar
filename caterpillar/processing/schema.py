# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""
Indexes (see :mod:`caterpillar.processing.index`) in caterpillar must have a :class:`.Schema`. This module defines that
schema and also provides a bunch of utility functions for working with schemas and csv.

"""
from __future__ import absolute_import, division

import logging
import re
import sys

import nltk
import ujson as json

from caterpillar.processing.analysis.analyse import EverythingAnalyser, \
    DefaultAnalyser, DateTimeAnalyser
from caterpillar.processing.analysis.tokenize import Token, ParagraphTokenizer
from caterpillar.test_util import TestAnalyser


logger = logging.getLogger(__name__)


class FieldConfigurationError(Exception):
    """There is a problem with the configuration of the field."""


class NonIndexedFieldError(ValueError):
    """The field is not a searchable indexed field. """


class UnknownFieldError(ValueError):
    """The field is not defined on the index. """


class UnsupportedOperatorError(ValueError):
    """The operator is not supported for the given field."""


class NonSearchableOperatorError(ValueError):
    """The operator is valid for the field, but not supported for search."""


class FieldType(object):
    """
    Represents a field configuration. :class:`.Schema`s are built out of fields.

    The FieldType object controls how a field is analysed via the ``analyser``
    (:class:`caterpillar.processing.analysis.Analyser`) attribute.

    If you don't provide an analyser for your field, it will default to a
    :class:``caterpillar.processing.analysis.EverythingAnalyser``.

    """
    # Convenience hash of operators -> methods
    FIELD_OPS = {'<': 'lt', '<=': 'lte', '>': 'gt', '>=': 'gte', '*=': 'equals_wildcard', '=': 'equals'}

    def __init__(self, analyser=EverythingAnalyser(), indexed=False, categorical=False, stored=True):
        """
        Create a new field.

        ``analyser`` (:class:`caterpillar.processing.analysis.Analyser`) is the analyser used for this field.
        ``indexed`` (bool) indicates if this field should be indexed or not. ``categorical`` (bool) indicates if this
        field is categorical or not. Categorical fields only support indexing for the purpose of searching and do not
        collect full statistics such as positions and associations. ``stored`` (bool) says if this field should be
        stored or not.

        """
        self._analyser = analyser
        self._indexed = indexed
        self._categorical = categorical
        self._stored = stored

    def analyse(self, value):
        """Analyse ``value``, returning a :class:`caterpillar.processing.analysis.tokenize.Token` generator."""
        for token in self._analyser.analyse(value):
            yield token

    @property
    def categorical(self):
        return self._categorical

    @property
    def indexed(self):
        return self._indexed

    @property
    def stored(self):
        return self._stored

    def evaluate_op(self, operator, value1, value2):
        """
        Evaluate ``operator`` (str from :const:`FieldType.FIELD_OPS`) on operands ``value1`` and ``value2``.

        """
        op_method = getattr(self, FieldType.FIELD_OPS[operator])
        return op_method(value1, value2)

    def equals(self, value1, value2):
        """Returns whether ``value1`` is equal to ``value2``."""
        raise NotImplementedError('Equality operator not supported for field type {}.'.format(self.__class__.__name__))

    def equals_wildcard(self, value, wildcard_value):
        """Returns whether ``value`` matches regex ``wildcard_value``."""
        raise NotImplementedError('Wildcard equality operator not supported for field type {}.'
                                  .format(self.__class__.__name__))

    def gt(self, value1, value2):
        """Returns whether ``value1`` is greater than ``value2``."""
        raise NotImplementedError('Greater-than operator not supported for field type {}.'
                                  .format(self.__class__.__name__))

    def gte(self, value1, value2):
        """Returns whether ``value1`` is greater than or equal to ``value2``."""
        raise NotImplementedError('Greater-than-or-equals operator not supported for field type {}.'
                                  .format(self.__class__.__name__))

    def lt(self, value1, value2):
        """Returns whether ``value1`` is less than ``value2``."""
        raise NotImplementedError('Less-than operator not supported for field type {}.'
                                  .format(self.__class__.__name__))

    def lte(self, value1, value2):
        """Returns whether ``value1`` is less than or equal to ``value2``."""
        raise NotImplementedError('Less-than-or-equals operator not supported for field type {}.'
                                  .format(self.__class__.__name__))


class CategoricalFieldType(FieldType):
    """Represents a categorical field type. Categorical fields can extend this class for convenience."""
    def __init__(self, analyser=EverythingAnalyser(), indexed=False, stored=True):
        super(CategoricalFieldType, self).__init__(analyser=analyser, indexed=indexed, categorical=True, stored=stored)

    def value_of(self, raw_value):
        """Return the value of ``raw_value`` after being processed by this field type's analyse method."""
        return list(self.analyse(raw_value))[0].value

    def equals(self, value1, value2):
        return self.value_of(value1) == self.value_of(value2)


class ID(CategoricalFieldType):
    """
    Configured field type that indexes the entire value of the field as one token. This is useful for data you don't
    want to tokenize, such as the path of a file.

    """
    def __init__(self, indexed=False, stored=True):
        super(ID, self).__init__(indexed=indexed, stored=stored)


class NUMERIC(CategoricalFieldType):
    """Special field type that lets you index ints or floats."""
    TYPES = (int, float)

    def __init__(self, indexed=False, stored=True, num_type=int, default_value=None):
        """Create new NUMERIC instance with type ``num_type`` (float or int) and default_value (float or int)."""
        if num_type not in NUMERIC.TYPES:
            raise ValueError("Invalid num_type '{}'".format(num_type))
        self._num_type = num_type
        self._default_value = default_value
        super(NUMERIC, self).__init__(analyser=None, indexed=indexed, stored=stored)

    def analyse(self, value):
        try:
            yield Token(self._num_type(value))
        except (TypeError, ValueError) as e:
            if value is None or len(value) == 0:
                yield Token(self._default_value)
            else:
                raise e

    def gt(self, value1, value2):
        return self.value_of(value1) > self.value_of(value2)

    def gte(self, value1, value2):
        return self.value_of(value1) >= self.value_of(value2)

    def lt(self, value1, value2):
        return self.value_of(value1) < self.value_of(value2)

    def lte(self, value1, value2):
        return self.value_of(value1) <= self.value_of(value2)


class BOOLEAN(CategoricalFieldType):
    """
    bool field type that lets you index boolean values (True and False).

    The field converts the bool values to text for you before indexing.

    """
    def __init__(self, indexed=False, stored=True):
        super(BOOLEAN, self).__init__(analyser=None, indexed=indexed, stored=stored)

    def analyse(self, value):
        yield Token(bool(value))


class TEXT(FieldType):
    """Configured field type for text fields."""
    def __init__(self, analyser=DefaultAnalyser(), indexed=True, stored=True):
        """
        Create a text field with ``analyser`` (:class:`caterpillar.processing.analysis.Analyser`) default to
        :class:`caterpillar.processing.analysis.DefaultAnalyzer`.

        """
        super(TEXT, self).__init__(analyser=analyser, indexed=indexed, categorical=False, stored=stored)


class CATEGORICAL_TEXT(CategoricalFieldType):
    """Configured field type for categorical text fields."""
    def __init__(self, indexed=False, stored=True):
        super(CATEGORICAL_TEXT, self).__init__(indexed=indexed, stored=stored)

    def analyse(self, value):
        yield Token(value.strip())

    def equals_wildcard(self, value, wildcard_value):
        return re.match(wildcard_value, value) is not None


class _TestText(TEXT):
    """A text field type used for testing."""
    def __init__(self, analyser=TestAnalyser(), indexed=True, stored=True):
        """
        Create a field similar to TEXT, but with controlled parameters for testing.

        This should not be used other than testing.

        """
        super(_TestText, self).__init__(analyser=analyser, indexed=indexed, stored=stored)


class DATETIME(FieldType):
    """Field type for datetimes.

    Datetimes are stored as ISO8601 strings down to a resolution of 1 second. All datetimes are either datetime
    unaware (no UTC offset) or datetime aware and stored as UTC (explicit offset +00:00). This means all datetimes
    are lexicographically comparable.

    """

    def __init__(self, analyser=DateTimeAnalyser(), indexed=False, stored=True):
        super(DATETIME, self).__init__(analyser=analyser, indexed=indexed, stored=stored, categorical=True)

    def value_of(self, raw_value):
        """Return the value of ``raw_value`` after being processed by this field type's analyse method."""
        return list(self.analyse(raw_value))[0].value

    def gt(self, value1, value2):
        return self.value_of(value1) > self.value_of(value2)

    def gte(self, value1, value2):
        return self.value_of(value1) >= self.value_of(value2)

    def lt(self, value1, value2):
        return self.value_of(value1) < self.value_of(value2)

    def lte(self, value1, value2):
        return self.value_of(value1) <= self.value_of(value2)

    def equals(self, value1, value2):
        return self.value_of(value1) == self.value_of(value2)


class Schema(object):
    """
    Represents the collection of fields in an index. Maps field names to FieldType objects which define the behavior of
    each field.

    Low-level parts of the index use field numbers instead of field names for compactness. This class has several
    methods for converting between the field name, field number, and field object itself.

    """
    def __init__(self, **fields):
        """
        All keyword arguments to the constructor are treated as ``fieldname = fieldtype`` pairs. The fieldtype can be an
        instantiated FieldType object, or a FieldType sub-class (in which case the Schema will instantiate it with the
        default constructor before adding it).

        For example::

            s = Schema(content = TEXT, title = TEXT(stored = True), tags = KEYWORD(stored = True))

        """

        self._fields = {}

        for name in sorted(fields.keys()):
            self.add(name, fields[name])

    def __iter__(self):
        """Returns the field objects in this schema."""
        return iter(self._fields.values())

    def __getitem__(self, name):
        """Returns the field associated with the given field name."""
        if name in self._fields:
            return self._fields[name]

        raise KeyError("No field named {}".format(name))

    def __len__(self):
        """Returns the number of fields in this schema."""
        return len(self._fields)

    def __contains__(self, field_name):
        """Returns True if a field by the given name is in this schema."""
        # Defined in terms of __getitem__ so that there's only one method to override to provide dynamic fields
        try:
            field = self[field_name]
            return field is not None
        except KeyError:
            return False

    def items(self):
        """Returns a list of ``("field_name", field_object)`` pairs for the fields in this schema."""
        return sorted(self._fields.items())

    def names(self):
        """Returns a list of the names of the fields in this schema."""
        return sorted(self._fields.keys())

    def get_indexed_text_fields(self):
        """Returns a list of the indexed text fields."""
        return [name for name, field in self._fields.iteritems() if field.indexed and type(field) == TEXT]

    def get_indexed_structured_fields(self):
        """Returns a list of the indexed structured (non-text) fields."""
        return [name for name, field in self._fields.iteritems() if field.indexed and type(field) != TEXT]

    def add(self, name, field_type):
        """
        Adds a field to this schema.

        ``name`` (str) is the name of the field. ``fieldtype`` (:class:`FieldType`) is either
        instantiated FieldType object, or a FieldType subclass. If you pass an instantiated object, the schema will use
        that as the field configuration for this field. If you pass a FieldType subclass, the schema will automatically
        instantiate it with the default constructor.

        """
        # Check field name
        if name.startswith("_"):
            raise FieldConfigurationError("Field names cannot start with an underscore")
        if name in self._fields:
            raise FieldConfigurationError("Schema already has a field {}".format(name))

        # If the user passed a type rather than an instantiated field object,
        # instantiate it automatically
        if type(field_type) is type:
            try:
                field_type = field_type()
            except:
                e = sys.exc_info()[1]
                raise FieldConfigurationError("Error: {} instantiating field {}: {}".format(e, name, field_type))

        if not isinstance(field_type, FieldType):
            raise FieldConfigurationError("{} is not a FieldType object".format(field_type))

        self._fields[name] = field_type


class CaterpillarLegacySchema(object):
    """
    An adapter for Schema objects to support the newer API.

    This allows migration from the previous mechanism of pickling objects outside the transactional mechanism
    of a writer to the current mechanism of declaring state that is saved inside the index.

    Also supports returning an old Schema object for some forward/backwards compatability.

    Currently this is a very thin skin implementing a very similar API, expect this to change in the future.

    Limitations:
        - Only the default analysers for each FieldType are supported.

    """

    field_to_config = {
        TEXT: 'text',
        NUMERIC: 'numeric',
        ID: 'id',
        BOOLEAN: 'boolean',
        CATEGORICAL_TEXT: 'categorical_text',
        DATETIME: 'datetime',
        _TestText: 'test_text'
    }
    config_to_field = {config: field for field, config in field_to_config.items()}

    def __init__(self, **fields):
        """
        All keyword arguments to the constructor are treated as ``fieldname = {config_key: config_val}`` pairs.

        """

        self.__schema = Schema()
        self.config = {}
        self.add_fields(**fields)

    def __iter__(self):
        """Returns the field objects in this schema."""
        return iter(self.__schema)

    def __getitem__(self, name):
        """Returns the field associated with the given field name."""
        return self.__schema[name]

    def __len__(self):
        """Returns the number of fields in this schema."""
        return len(self.__schema)

    def __contains__(self, field_name):
        """Returns True if a field by the given name is in this schema."""
        return field_name in self.__schema

    def items(self):
        """Returns a list of ``("field_name", field_object)`` pairs for the fields in this schema."""
        return self.__schema.items()

    def names(self):
        """Returns a list of the names of the fields in this schema."""
        return self.__schema.names()

    def get_indexed_text_fields(self):
        """Returns a list of the indexed text fields."""
        return self.__schema.get_indexed_text_fields()

    def get_indexed_structured_fields(self):
        """Returns a list of the indexed structured (non-text) fields."""
        return self.__schema.get_indexed_structured_fields()

    def add_fields(self, **fields):

        for name in sorted(fields.keys()):
            if name in self.config:
                raise FieldConfigurationError('Field {} already exists in this schema'.format(name))
            config = fields[name]
            self.config[name] = config
            config_filtered = {
                name: value for name, value in config.items()
                if name in ['indexed', 'stored', 'categorical']
            }
            self.__schema.add(
                name, CaterpillarLegacySchema.config_to_field[config['type']](**config_filtered)
            )

    @classmethod
    def parse_legacy_schema(cls, schema):
        """ Takes an instance of a `schema` object, and returns a config for a new declarative style schema. """
        field_config = {}
        for field_name, field_object in schema.items():

            this_field = {}
            this_field['indexed'] = field_object.indexed
            this_field['stored'] = field_object.stored

            try:
                this_field['type'] = CaterpillarLegacySchema.field_to_config[type(field_object)]
            except KeyError:
                raise FieldConfigurationError('Could not match field to a known FieldType object')

            field_config[field_name] = this_field

        return field_config

    def get_legacy_schema(self):
        """ Return a legacy schema object for storing in the file system for forwards compatability."""

        return self.__schema

    def analyse(self, document, frame_size=2, encoding='utf-8', encoding_errors='strict'):
        """
        Analyse the document, returning a break down of fields, values and frames to be stored.

        We index :class:`TEXT <caterpillar.schema.TEXT>` fields by breaking them into frames for analysis. The
        ``frame_size`` (int) param controls the size of those frames. Setting ``frame_size`` to an int < 1 will result
        in all text being put into one frame or, to put it another way, the text not being broken up into frames.

        .. note::
            Because we always store a full positional index with each index, we are still able to do document level
            searches like TF/IDF even though we have broken the text down into frames. So, don't fret!

        ``encoding`` (str) and ``encoding_errors`` (str) are passed directly to :meth:`str.decode()` to decode the data
        for all :class:`TEXT <caterpillar.schema.TEXT>` fields. Refer to its documentation for more information.

        ``**fields`` is the fields and their values for this document. Calling this method will look something like
        this::

            >>> writer.add_document(field1=value1, field2=value2).

        Any unrecognized fields are just ignored.

        Raises :exc:`TypeError` if something other then str or bytes is given for a TEXT field and :exec:`IndexError`
        if there are any problems decoding a field.

        """

        schema_fields = self.__schema.items()
        sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

        # Build the frames by performing required analysis.
        frames = {}  # Frame data:: field_name -> [frame1, frame2, frame3]
        term_positions = {}  # Term vector data:: field_name --> [{term1:freq, term2:freq}, {term2:freq, term3:freq}]

        metadata = {}  # Inverted frame metadata:: field_name -> field_value

        # Shell frame includes all non-indexed and categorical fields
        shell_frame = {}
        for field_name, field in schema_fields:
            if (not field.indexed or field.categorical) and field.stored and field_name in document:
                shell_frame[field_name] = document[field_name]

        # Tokenize fields that need it
        logger.debug('Starting tokenization of document')
        frame_count = 0

        # Analyze document level structured fields separately to inject in the frames.
        for field_name, field in schema_fields:

            if field_name not in document or document[field_name] is None \
                    or not field.indexed or not field.categorical:
                # Skip fields not supplied or with empty values for this document.
                continue

            # Record categorical values
            for token in field.analyse(document[field_name]):
                metadata[field_name] = token.value

        # Now just the unstructured fields
        for field_name, field in schema_fields:

            if field_name not in document or document[field_name] is None \
                    or not field.indexed or field.categorical:
                continue

            # Start the index for this field
            frames[field_name] = []
            term_positions[field_name] = []

            # Index non-categorical fields
            field_data = document[field_name]
            expected_types = (str, bytes, unicode)
            if isinstance(field_data, str) or isinstance(field_data, bytes):
                try:
                    field_data = document[field_name] = field_data.decode(encoding, encoding_errors)
                except UnicodeError as e:
                    raise IndexError("Couldn't decode the {} field - {}".format(field_name, e))
            elif type(field_data) not in expected_types:
                raise TypeError("Expected str or bytes or unicode for text field {} but got {}".
                                format(field_name, type(field_data)))
            if frame_size > 0:
                # Break up into paragraphs
                paragraphs = ParagraphTokenizer().tokenize(field_data)
            else:
                # Otherwise, the whole document is considered as one paragraph
                paragraphs = [Token(field_data)]

            for paragraph in paragraphs:
                # Next we need the sentences grouped by frame
                if frame_size > 0:
                    sentences = sentence_tokenizer.tokenize(paragraph.value, realign_boundaries=True)
                    sentences_by_frames = [sentences[i:i + frame_size]
                                           for i in xrange(0, len(sentences), frame_size)]
                else:
                    sentences_by_frames = [[paragraph.value]]
                for sentence_list in sentences_by_frames:
                    token_position = 0
                    # Build our frames
                    frame = {
                        '_field': field_name,
                        '_positions': {},
                        '_sequence_number': frame_count,
                        '_metadata': metadata  # Inject the document level structured data into the frame
                    }
                    if field.stored:
                        frame['_text'] = " ".join(sentence_list)
                    for sentence in sentence_list:
                        # Tokenize and index
                        tokens = field.analyse(sentence)

                        # Record positional information
                        for token in tokens:
                            # Add to the list of terms we have seen if it isn't already there.
                            if not token.stopped:
                                # Record word positions
                                try:
                                    frame['_positions'][token.value].append(token_position)
                                except KeyError:
                                    frame['_positions'][token.value] = [token_position]

                            token_position += 1

                    # Build the final frame and add to the index
                    frame.update(shell_frame)
                    # Serialised representation of the frame
                    frames[field_name].append(json.dumps(frame))

                    # Generate the term-frequency vector for the frame:
                    term_positions[field_name].append(frame['_positions'])

        # Finally add the document to storage.
        doc_fields = {}

        for field_name, field in schema_fields:
            if field.stored and field_name in document:
                # Only record stored fields against the document
                doc_fields[field_name] = document[field_name]

        doc_repr = json.dumps(doc_fields)

        return ('v1', (doc_repr, metadata, frames, term_positions))
