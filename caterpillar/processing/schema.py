# caterpillar: Schema for CSV uploads
#
# Copyright (C) 2012-2013 Mammoth Labs
# Kris Rogers <kris@mammothlabs.com.au>
import csv
import regex


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


AVG_WORDS_TEXT = 5  # Minimum number of average words per row to consider a column as text
NUM_PEEK_ROWS_CSV = 20  # Number of csv rows to consider when generating automatic schema

def generate_csv_schema(csv_file, delimiter=','):
    """
    Attempt to generate a schema for the csv file automatically.

    Required Arguments:
    csv_file -- The CSV file to generate a schema for.

    Optional Arguments:
    delimiter -- CSV delimiter character.

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
    if sniffer.has_header(snipit):
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
    csv_schema = []
    for index, stats in column_stats.items():
        name = index
        if headers and index < len(headers):
            name = headers[index]
        if stats['total_words'] / NUM_PEEK_ROWS_CSV >= AVG_WORDS_TEXT:
            # Enough words for a text column
            csv_schema.append(ColumnSpec(name, ColumnDataType.TEXT))
        else:
            # Didn't match anything, default to IGNORE
            csv_schema.append(ColumnSpec(name, ColumnDataType.IGNORE))

    return csv_schema, sample_rows