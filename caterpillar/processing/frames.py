# caterpillar: Tools to work with and extract text frames
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>, Kris Rogers <kris@mammothlabs.com.au>
from StringIO import StringIO
import csv

import uuid
from caterpillar.processing.tokenize import ParagraphTokenizer

import nltk.data


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

        Required
        name -- A string name for this column.
        type -- The type of this column as a ColumnDataType object.

        """
        self.name = name
        self.type = type


class Frame(object):
    """
    A frame is a piece of text who's size is measured in sentences.

    A frame has a minimum size of 1. A frame also has additional information about itself including a dict of word
    frequencies, a dict of metadata, a string representation of its original form, a unique identifier, a sequence
    number and optionally a list of unique words.

    """

    def __init__(self, id, sequence, text, metadata=None, unique_words=None):
        """
        Create a new frame from the passed id string, sequence int, text string, frequencies dict and optional metadata
        dict, unique_words dict.

        """
        self.id = id
        self.text = text
        self.sequence = sequence
        self.metadata = metadata
        self.unique_words = unique_words


WINDOW_SIZE = 1024*1024*10  # our sliding window of text will be 10MB big


def frame_stream(text_file, frame_size=2, tokenizer=nltk.data.load('tokenizers/punkt/english.pickle'), meta_data=None,
                 encoding='utf-8'):
    """
    This generator function yields text frames parsed from text_file.

    A frame is defined as a block of text who's size is measured in sentences and is at least on sentence long. A frame
    can have metadata associated with it. Some of this metadata is passed to this function directly (original document
    name as text for example or maybe even document author). Other metadata could possibly be emergent from the text
    frames themselves.

    Required arguments:
    text_file -- A file object returned by something like open() where the text data is to be read from.

    Keyword arguments:
    frame_size -- The size of the frames to yield as an int (default 2). If this argument is less than 1 then only
                  1 frame is returned containing all the text in the passed file like object.
    tokenizer -- An object instance with a tokenize method that accepts a string and returns sentences.
    meta_data -- A dict of meta data values.
    encoding -- The encoding of the strings read from text_file.

    """
    sequence_number = 1
    if frame_size > 0:
        # Break text up into frames of frame_size sentences long
        window = u""
        input = text_file.read(WINDOW_SIZE)
        while input:
            window += input.decode(encoding)
            paragraphs = ParagraphTokenizer().tokenize(window)
            for paragraph in paragraphs[:-1]:  # Never tokenize the last paragraph in case it isn't complete
                sentences = tokenizer.tokenize(paragraph, realign_boundaries=True)
                frames_text = (" ".join(sentences[i:i+frame_size]) for i in xrange(0, len(sentences), frame_size))
                for text in frames_text:
                    yield Frame(uuid.uuid4(), sequence_number, text, meta_data)
                    sequence_number += 1
            window = window[window.rfind(paragraphs[-1]):]
            window = window.lstrip()  # Don't want it starting with spaces!
            input = text_file.read(WINDOW_SIZE - len(window))
        paragraphs = ParagraphTokenizer().tokenize(window)
        for paragraph in paragraphs:
            sentences = tokenizer.tokenize(paragraph, realign_boundaries=True)
            frames_text = (" ".join(sentences[i:i+frame_size]) for i in xrange(0, len(sentences), frame_size))
            for text in frames_text:
                yield Frame(uuid.uuid4(), sequence_number, text, meta_data)
                sequence_number += 1
    else:
        # Return all text in 1 frame
        input = text_file.read().decode(encoding)
        yield Frame(uuid.uuid4(), sequence_number, input.strip(), meta_data)


def frame_stream_csv(csv_file, column_spec, frame_size=2, tokenizer=nltk.data.load('tokenizers/punkt/english.pickle'),
                     meta_data=dict(), encoding='utf-8', delimiter=',', quotechar='"'):
    """
    This generator function yields text frames parsed from csv_file.

    A frame is defined as a block of text who's size is measured in sentences and is at least on sentence long. A frame
    can have metadata associated with it. Some of this metadata is passed to this function directly (original document
    name as text for example or maybe even document author). Other metadata could be emergent from the text frames
    themselves. In the case of CSV files, all values in non-textual cells will be captured as metadata.

    To parse a CSV file this function needs a column_spec which is a list of ColumnSpec objects. The order of the
    ColumnSpec objects in the passed list must correspond to the order of columns in the passed csv_file. All columns
    must be accounted for in the passed ColumnSpec list. If you aren't interested in a column, set it to
    ColumnDataType.IGNORE.

    Required arguments:
    csv_file -- A file like object returned by something like open() where the text data is to be read from. If this
                is a file object it must be opened with the 'rbU' flag.
    column_spec -- A list of ColumnSpec objects in column order.

    Keyword arguments:
    frame_size -- The size of the frames to yield from text cells as an int (default 2). If this argument is less than 1
                  then all the text in a cell will be returned as a single frame.
    tokenizer -- An object instance with a tokenize method that accepts a string and returns sentences.
    meta_data -- A dict of meta data values for this file.
    encoding -- The encoding of the strings read from text_file.
    delimiter -- A one-character string used to separate fields. It defaults to ','.

    """
    # Try and guess a dialect by parsing a sample
    snipit = csv_file.read(4096)
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(snipit, delimiter)

    # Reset back to the start of the file and actually read it
    csv_file.seek(0)
    csv_reader = csv.reader(csv_file, dialect)
    # Don't parse the header
    if sniffer.has_header(snipit):
        csv_reader.next()
    # Do the actual work. Go through row-by-row then cell-by-cell looking at the data type for each cell. If it is a
    # TEXT cell, then add it to a queue for this row. Otherwise, if it isn't an IGNORE cell, add it to the meta data
    # for this row. Then, return to the queue of TEXT columns and extract teh frames from each passing in the discovered
    # meta data.
    row_seq = 1  # Might be interesting to have, so store it
    for row in csv_reader:
        row_meta_data = meta_data.copy()
        row_meta_data['row_seq'] = str(row_seq)
        row_seq += 1
        text_queue = []
        index = 0
        for cell in row:
            if column_spec[index].type != ColumnDataType.IGNORE and cell:
                if column_spec[index].type == ColumnDataType.TEXT:
                    text_queue.append((column_spec[index].name, cell))
                else:
                    row_meta_data[column_spec[index].name] = cell
            index += 1
        # Process the TEXT cells
        for text_cell in text_queue:
            cell_meta_data = row_meta_data.copy()
            cell_meta_data['column'] = text_cell[0]
            for frame in frame_stream(StringIO(text_cell[1]), frame_size=frame_size, tokenizer=tokenizer,
                                      meta_data=cell_meta_data, encoding=encoding):
                yield frame