# caterpillar: Schema for CSV uploads
#
# Copyright (C) 2012-2013 Mammoth Labs
# Kris Rogers <kris@mammothlabs.com.au>
from __future__ import division
import csv
import regex
from StringIO import StringIO


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


MAX_HEADER_SIZE_PERCENTAGE = 0.33    # Maximum size for header row as a percentage of the average row size for following rows
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
        if checked == 20:
            break
        total_row_size += sum([len(col) for col in row])
        checked += 1
    avg_row_size = total_row_size / checked

    return header_size / avg_row_size <= MAX_HEADER_SIZE_PERCENTAGE