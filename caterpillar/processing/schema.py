# caterpillar: Schema for documents
#
# Copyright (C) 2012-2013 Mammoth Labs
# Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@stuart.id.au>
from __future__ import division
import abc
import csv
from StringIO import StringIO
import sys

import regex

from caterpillar.processing.analysis.analyse import EverythingAnalyser, DefaultAnalyser


class FieldConfigurationError(Exception):
    pass


class FieldType(object):
    """
    Represents a field configuration. Schemas are built out of fields.

    The FieldType object supports the following attributes:

    * analyzer (analysis.Analyzer): the analyzer to use to turn text into terms.

    If you don't provide an analyser for your field, it will default to a ``EverythingAnalyser``.

    """
    def __init__(self, analyser=EverythingAnalyser(), indexed=False):
        """
        Optional Arguments:
        analyser -- the ``Analyser`` for this field.
        indexed -- a boolean flag indicating if this field should be indexed or not.
        """
        self._analyser = analyser
        self._indexed = indexed

    def analyse(self, value):
        """
        Use the ``Analyser`` for this field to return tokens. This method yields tokens from the ``Analyser``'s
        analyse() method.

        Required Arguments:
        value -- the value of the field to analyse.

        """
        for token in self._analyser.analyse(value):
            yield token

    def indexed(self):
        """
        Should this field be indexed?

        """
        return self._indexed

    def on_remove(self, schema, name):
        """
        Perform some action when the field is removed from a schema.

        Required Arguments:
        schema -- the ``Schema`` object this field is being removed from.
        name -- the str name of the field being removed.

        """
        return


class ID(FieldType):
    """
    Configured field type that indexes the entire value of the field as one token. This is useful for data you don't
    want to tokenize, such as the path of a file.

    """
    pass


class NUMERIC(FieldType):
    """
    Special field type that lets you index integer or floating point numbers.

    """
    pass


class BOOLEAN(FieldType):
    """
    Special field type that lets you index boolean values (True and False). The field converts the boolean values to
    text for you before indexing.

    >>> schema = Schema(path=STORED, done=BOOLEAN)
    >>> ix = storage.create_index(schema)
    >>> w = ix.writer()
    >>> w.add_document(path="/a", done=False)
    >>> w.commit()
    """
    pass


class TEXT(FieldType):
    """
    Configured field type for text fields.

    """
    def __init__(self, analyser=DefaultAnalyser(), indexed=True):
        """
        Optional Arguments:
        analyzer -- The processing.analysis.Analyser to use to index the field contents. If you omit this argument, the
        field uses processing.analysis.DefaultAnalyzer.

        """
        super(TEXT, self).__init__(analyser=analyser, indexed=indexed)


class Schema(object):
    """
    Represents the collection of fields in an index. Maps field names to FieldType objects which define the behavior of
    each field.

    Low-level parts of the index use field numbers instead of field names for compactness. This class has several
    methods for converting between the field name, field number, and field object itself.

    """
    def __init__(self, **fields):
        """
        All keyword arguments to the constructor are treated as fieldname = fieldtype pairs. The fieldtype can be an
        instantiated FieldType object, or a FieldType sub-class (in which case the Schema will instantiate it with the
        default constructor before adding it).

        For example::

            s = Schema(content = TEXT, title = TEXT(stored = True), tags = KEYWORD(stored = True))

        """

        self._fields = {}

        for name in sorted(fields.keys()):
            self.add(name, fields[name])

    def __iter__(self):
        """
        Returns the field objects in this schema.

        """
        return iter(self._fields.values())

    def __getitem__(self, name):
        """
        Returns the field associated with the given field name.

        """
        if name in self._fields:
            return self._fields[name]

        raise KeyError("No field named {}".format(name))

    def __len__(self):
        """
        Returns the number of fields in this schema.

        """
        return len(self._fields)

    def __contains__(self, field_name):
        """
        Returns True if a field by the given name is in this schema.

        """
        # Defined in terms of __getitem__ so that there's only one method to
        # override to provide dynamic fields
        try:
            field = self[field_name]
            return field is not None
        except KeyError:
            return False

    def items(self):
        """
        Returns a list of ("field_name", field_object) pairs for the fields in this schema.

        """
        return sorted(self._fields.items())

    def names(self):
        """
        Returns a list of the names of the fields in this schema.

        """
        return sorted(self._fields.keys())

    def add(self, name, field_type):
        """
        Adds a field to this schema.

        Required Arguments:
        name -- The string name of the field.
        fieldtype -- An instantiated fields.FieldType object, or a FieldType subclass. If you pass an instantiated
                     object, the schema will use that as the field configuration for this field. If you pass a FieldType
                     subclass, the schema will automatically instantiate it with the default constructor.

        """
        # Check field name
        if name.startswith("_"):
            raise FieldConfigurationError("Field names cannot start with an underscore")
        if " " in name:
            raise FieldConfigurationError("Field names cannot contain spaces")
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

    def remove(self, field_name):
        if field_name in self._fields:
            self._fields[field_name].on_remove(self, field_name)
            del self._fields[field_name]
        else:
            raise KeyError("No field named {}".format(field_name))


class ColumnDataType(object):
    """
    A ColumnDataType object identifies different data types in a CSV column.

    There are five possible data types:
    FLOAT -- A floating point number. Should be a string in a format that float() can parse.
    INTEGER -- A integer. Should be in a format that int() can parse.
    STRING -- A string type. This type ISN'T analysed. It is just stored.
    TEXT -- A string type. This is like STRING but it IS analysed and stored (it is used to generate a frame stream).
    IGNORE -- Ignore this column.

    """
    FLOAT = 1
    INTEGER = 2
    STRING = 3
    TEXT = 4
    IGNORE = 5


class ColumnSpec(object):
    """
    A ColumnSpec object represents information about a column in a CSV file.

    This includes the column's name and its type.

    """

    def __init__(self, name, type):
        """
        Create a new ColumnSpec. A ColumnSpec must have a name and a type.

        Required Arguments:
        name -- A string name for this column.
        type -- The type of this column as a ColumnDataType object.

        """
        self.name = name
        self.type = type


class CsvSchema(object):
    """
    This class represents the schema required to process a particular CSV data file.

    Required Arguments:
    columns -- A list of ``ColumnSpec`` objects to define how the data should be processed.
    has_header -- A boolean indicating whether the first row of the file contains headers.
    dialect -- The dialect to use when parsing the file.

    Optional Arguments:
    sample_rows -- A list of row data that was used to generate the schema.

    """
    def __init__(self, columns, has_header, dialect, sample_rows=[]):
        self.columns = columns
        self.has_header = has_header
        self.dialect = dialect
        self.sample_rows = sample_rows


AVG_WORDS_TEXT = 5  # Minimum number of average words per row to consider a column as text
NUM_PEEK_ROWS_CSV = 20  # Number of csv rows to consider when generating automatic schema
def generate_csv_schema(csv_file, delimiter=',', encoding='utf8'):
    """
    Attempt to generate a schema for the csv file automatically.

    Required Arguments:
    csv_file -- The CSV file to generate a schema for.

    Optional Arguments:
    delimiter -- CSV delimiter character.
    encoding -- Character encoding of the file.

    Returns a 2-tuple containing the generated schema and the sample rows used to generate the schema.

    """
    # Try and guess a dialect by parsing a sample
    snipit = csv_file.read(4096)
    csv_file.seek(0)
    sniffer = csv.Sniffer()
    dialect = None
    try:
        dialect = sniffer.sniff(snipit, delimiter)
    except Exception:
        # Fall back to excel csv dialect
        dialect = csv.excel

    # Now actually read the file
    reader = csv.reader(csv_file, dialect)
    headers = []
    has_header = csv_has_header(snipit, dialect)
    if has_header:
        headers = reader.next()

    # Collect column statistics
    sample_rows = []
    column_stats = {}
    for i in range(NUM_PEEK_ROWS_CSV):
        try:
            row = reader.next()
        except StopIteration:
            # No more rows
            break
        sample_rows.append(row)
        for j in range(len(row)):
            col = row[j]
            if j not in column_stats:
                column_stats[j] = {'total_words':0}
            column_stats[j]['total_words'] += len(regex.findall(r'\w+', col))

    # Define columns and generate schema
    columns = []
    for index, stats in column_stats.items():
        if headers and index < len(headers):
            name = unicode(headers[index], encoding, errors='ignore')
        else:
            name = index + 1
        if stats['total_words'] / NUM_PEEK_ROWS_CSV >= AVG_WORDS_TEXT:
            # Enough words for a text column
            columns.append(ColumnSpec(name, ColumnDataType.TEXT))
        else:
            # Didn't match anything, default to IGNORE
            columns.append(ColumnSpec(name, ColumnDataType.IGNORE))

    return CsvSchema(columns, has_header, dialect, sample_rows)


MAX_HEADER_SIZE_PERCENTAGE = 0.33  # Maximum size for header row as a percentage of the average row size for following rows


def csv_has_header(sample, dialect):
    """
    Custom heuristic for recognising header in CSV files. Intended to be used as an alternative
    for the ``csv.Sniffer.has_header`` method which doesn't work well for mostly-text CSV files.

    The heuristic we use simply checks the total size of the header row compared to the average row size for
    the following rows. If a large discrepancy is found, we assume that the first row contains headers.

    Required Arguments:
    sample -- A sample of data from the CSV.
    dialect -- CSV dialect to use.

    """
    reader = csv.reader(StringIO(sample), dialect)
    header = reader.next() # assume first row is header
    header_size = sum([len(col) for col in header])

    # Compute average row size
    total_row_size = 0
    checked = 0
    for row in reader:
        if checked == 50:
            break
        total_row_size += sum([len(col) for col in row])
        checked += 1
    avg_row_size = total_row_size / checked

    return header_size / avg_row_size <= MAX_HEADER_SIZE_PERCENTAGE