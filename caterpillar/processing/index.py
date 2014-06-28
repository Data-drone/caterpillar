# caterpillar: Tools to create and store a text index
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@mammothlabs.com.au>

"""
All interactions with an index are handled by this module. In particular the `Index` class handles the creation and
modification of an index including the recovery of an existing index. For combining two indexes together for the purpose
of a statistical analysis (for example) you can use a `DerivedIndex`. This module also provides a utility method called
`find_bi_gram_words()` for detecting a list of bi-grams from a piece of text.

"""

from __future__ import division
import os
import random
import ujson as json
import logging
import uuid

import nltk

from caterpillar.data.storage import DuplicateContainerError, StorageNotFoundError
from caterpillar.data.sqlite import SqliteMemoryStorage
from caterpillar.processing.analysis.analyse import PotentialBiGramAnalyser
from caterpillar.processing.analysis.tokenize import ParagraphTokenizer, Token
from caterpillar.processing.schema import Schema
from caterpillar.searching import IndexSearcher
from caterpillar.searching.scoring import TfidfScorer


logger = logging.getLogger(__name__)


class DocumentNotFoundError(Exception):
    """No document by that name exists."""


class SettingNotFoundError(Exception):
    """No setting by that name exists."""


class IndexNotFoundError(Exception):
    """No index exists at specified location."""


class TermNotFoundError(Exception):
    """Term doesn't exist in index."""


class Index(object):
    """
    An index of all the text within a data set; holds statistical information to be used in retrieval and analytics.

    Required Arguments:
    schema -- a ``schema.Schema`` instance that describes the structure of documents that reside in this index.
    data_storage -- a ``data.storage.Storage`` instance that will be used to write and read document, frame and config
    data.
    results_storage -- a ``data.storage.Storage`` instance that will be used to write and read processing results.

    """

    # Data storage
    DATA_STORAGE = "data.db"
    DOCUMENTS_CONTAINER = "documents"
    FRAMES_CONTAINER = "frames"
    SETTINGS_CONTAINER = "settings"
    SETTINGS_SCHEMA = "schema"
    INFO_CONTAINER = "info"

    # Results storage
    RESULTS_STORAGE = "results.db"
    ASSOCIATIONS_CONTAINER = "associations"
    FREQUENCIES_CONTAINER = "frequencies"
    METADATA_CONTAINER = "metadata"
    POSITIONS_CONTAINER = "positions"

    def __init__(self, schema, data_storage, results_storage):
        self._schema = schema
        self._data_storage = data_storage
        self._results_storage = results_storage

        # Store schema
        self._data_storage.set_container_item(Index.SETTINGS_CONTAINER, Index.SETTINGS_SCHEMA, schema.dumps())
        # Set state
        self.set_clean(True)
        self.update_revision()
        data_storage.set_container_item(Index.INFO_CONTAINER, 'derived', json.dumps(False))

    @staticmethod
    def create(schema, path=None, storage_cls=SqliteMemoryStorage, **args):
        """
        Create a new index object with the specified path, schema and a instance of the passed storage class.

        Required Arguments:
        schema -- the ``Schema`` for the index.

        Optional Arguments:
        path -- path to store index data under. MUST be specified if persistent storage is used.
        storage_cls -- the class to instantiate for the storage object. Defaults to ``SqliteMemoryStorage``.
        The create() method will be called on this instance.
        **args -- any keyword arguments that need to be passed onto the storage instance.
        """
        data_storage = storage_cls.create(Index.DATA_STORAGE, path=path, acid=True, containers=[
            Index.DOCUMENTS_CONTAINER,
            Index.FRAMES_CONTAINER,
            Index.SETTINGS_CONTAINER,
            Index.INFO_CONTAINER,
        ], **args)
        results_storage = storage_cls.create(Index.RESULTS_STORAGE, path=path, acid=False, containers=[
            Index.POSITIONS_CONTAINER,
            Index.ASSOCIATIONS_CONTAINER,
            Index.FREQUENCIES_CONTAINER,
            Index.METADATA_CONTAINER,
        ], **args)
        return Index(schema, data_storage, results_storage)

    @staticmethod
    def open(path, storage_cls):
        """
        Open an existing Index with the given path. Only supported for persistent storage backed Index.

        Required Arguments:
        path -- path to the stored Index.
        storage_cls -- class to instantiate for the storage object. The open() method will be called on this object.

        """
        try:
            data_storage = storage_cls.open(Index.DATA_STORAGE, path)
            results_storage = storage_cls.open(Index.RESULTS_STORAGE, path)
        except StorageNotFoundError:
            raise IndexNotFoundError("No index exists at path '{}".format(path))
        schema = Schema.loads(data_storage.get_container_item(Index.SETTINGS_CONTAINER, Index.SETTINGS_SCHEMA))
        return Index(schema, data_storage, results_storage)

    def destroy(self):
        """
        Permanently destroy this index.

        """
        self._data_storage.destroy()
        self._results_storage.destroy()

    def get_positions_index(self):
        """
        Returns a dict of term positions for this Index.

        This is what is known as an inverted text index. Structure is as follows::

        {
            term: {
                frame_id: [(start, end), (start, end)],
                ...
            },
            ...
        }

        """
        return {
            k: json.loads(v)
            for k, v in self._results_storage.get_container_items(Index.POSITIONS_CONTAINER).iteritems()
        }

    def get_term_positions(self, term):
        """
        Returns a dict of term positions for term.

        This is what is known as an inverted text index. Structure is as follows::

        {
            frame_id1: [(start, end), (start, end)],
            frame_id2: [(start, end), (start, end)],
            ...
        }

        """
        return json.loads(self._results_storage.get_container_item(Index.POSITIONS_CONTAINER, term))

    def get_associations_index(self):
        """
        Returns a dict of term associations for this Index.

        This is used to record when two terms co-occur in a document. Be aware that only 1 co-occurrence for two terms
        is recorded per document not matter the frequency of each term. The format is as follows::

        {
            term: {
                other_term: count,
                ...
            },
            ...
        }

        """
        return {
            k: json.loads(v)
            for k, v in self._results_storage.get_container_items(Index.ASSOCIATIONS_CONTAINER).iteritems()
        }

    def get_term_association(self, term, assc):
        """
        Returns a count of term associations between term and assc.

        Required Arguments:
        term -- the str term.
        assc -- the str associated term.

        """
        return json.loads(self._results_storage.get_container_item(Index.ASSOCIATIONS_CONTAINER, term))[assc]

    def get_frequencies(self):
        """
        Returns a dict of term frequencies for this Index.

        Be aware that a terms frequency is only incremented by 1 per document not matter the frequency within that
        document. The format is as follows::

        {
            term: count
        }

        """
        return {
            k: json.loads(v)
            for k, v in self._results_storage.get_container_items(Index.FREQUENCIES_CONTAINER).iteritems()
        }

    def get_term_frequency(self, term):
        """
        Return the frequency of term as an int.

        Required Arguments:
        term -- the str term.

        """
        return json.loads(self._results_storage.get_container_item(Index.FREQUENCIES_CONTAINER, term))

    def get_frame_count(self):
        """
        Return the count of frames stored on this index.

        """
        return self._data_storage.get_container_len(Index.FRAMES_CONTAINER)

    def get_frame(self, frame_id):
        """
        Fetch a frame by frame_id.

        Required Arguments:
        frame_id -- the str id of the frame.

        """
        return json.loads(self._data_storage.get_container_item(Index.FRAMES_CONTAINER, frame_id))

    def get_frames(self, frame_ids=None):
        """
        Fetch frames of this index.

        Optional Arguments:
        frame_ids -- a list of ids to filter frames by.

        """
        return {
            k: json.loads(v)
            for k, v in self._data_storage.get_container_items(Index.FRAMES_CONTAINER, keys=frame_ids).iteritems()
        }

    def get_frame_ids(self):
        """
        Return a list of ids for all frames stored on this index.

        """
        return self._data_storage.get_container_keys(Index.FRAMES_CONTAINER)

    def get_document(self, d_id):
        """
        Returns the document with the given d_id as a dict.

        Required Arguments:
        d_id -- the string id of the document.

        """
        try:
            return json.loads(self._data_storage.get_container_item(Index.DOCUMENTS_CONTAINER, d_id))
        except KeyError:
            raise DocumentNotFoundError("No document '{}'".format(d_id))

    def get_document_count(self):
        """
        Returns the count of documents added to this index.

        """
        return len(self._data_storage.get_container_items(Index.DOCUMENTS_CONTAINER))

    def get_documents(self):
        """
        Generator that yields all documents from this index as (key, value) tuples.

        """
        for k, v in self._data_storage.yield_container_items(Index.DOCUMENTS_CONTAINER):
            yield (k, json.loads(v))

    def get_metadata(self):
        """
        Returns index of metadata field -> value -> [frames].

        """
        return {
            k: json.loads(v)
            for k, v in self._results_storage.get_container_items(Index.METADATA_CONTAINER).iteritems()
        }

    def get_schema(self):
        """
        Get the schema for this index.

        """
        return self._schema

    def set_schema(self, schema):
        """
        Update the schema for this index.

        """
        self._data_storage.set_container_item(Index.SETTINGS_CONTAINER, Index.SETTINGS_SCHEMA, schema.dumps())
        self._schema = schema

    def add_fields(self, **fields):
        """
        Add new fields to the schema.

        All keyword arguments are treated as field name, field type pairs.

        """
        for name, value in fields.iteritems():
            self._schema.add(name, value)
        # Save updated schema to storage
        self._data_storage.set_container_item(Index.SETTINGS_CONTAINER, Index.SETTINGS_SCHEMA, self._schema.dumps())

    def get_revision(self):
        """
        Return the string revision identifier for this index.

        The revision identifier is a version identifier. It gets updated every time the index gets changed.

        """
        return self._data_storage.get_container_item(Index.INFO_CONTAINER, 'revision')

    def update_revision(self):
        """
        Updates the revision identifier for this index to reflect that the index has changed.

        """
        self._data_storage.set_container_item(Index.INFO_CONTAINER,
                                              'revision', json.dumps(random.SystemRandom().randint(0, 10**10)))

    def is_clean(self):
        """
        Returns whether this index is clean or not.

        A clean index is one that is up-to-date. That is, all frames have been added to the index data structures.

        """
        return json.loads(self._data_storage.get_container_item(Index.INFO_CONTAINER, 'clean'))

    def set_clean(self, clean=False):
        """
        Sets whether this index is clean or not.

        """
        self._data_storage.set_container_item(Index.INFO_CONTAINER, 'clean', clean)

    def get_vocab_size(self):
        """
        Get total number of unique terms identified for this index.

        """
        return self._results_storage.get_container_len(Index.POSITIONS_CONTAINER)

    def is_derived(self):
        """
        Return whether this index is derived.

        """
        return json.loads(self._data_storage.get_container_item(Index.INFO_CONTAINER, 'derived'))

    def searcher(self, scorer_cls=TfidfScorer):
        """
        Return a searcher for this Index.

        """
        return IndexSearcher(self, scorer_cls)

    def __sizeof__(self):
        """
        Get the size of this index in bytes.

        This is a compromise. It isn't possible to get the size of an SQLite in memory index. So, instead, we assume
        each frame is 10KB big. This was arrived at by looking at the size of the DB created after adding a large
        document and dividing by the number of frames and rounding to the nearest 10KB.

        If this index is using `SqliteStorage <caterpillar.data.sqlite.SqliteStorage>` then we just measure the size of
        its on-disk DBs.

        """
        if not isinstance(self._data_storage, SqliteMemoryStorage):
            data = os.path.getsize(self._data_storage.get_db_path())
            results = os.path.getsize(self._results_storage.get_db_path())
            return data + results
        else:
            return 10*1024*self.get_frame_count()

    def get_setting(self, name):
        """
        Get a setting identified by name.

        Required Arguments:
        name -- str name identifying the setting.

        """
        try:
            return json.loads(self._data_storage.get_container_item(Index.SETTINGS_CONTAINER, name))
        except KeyError:
            raise SettingNotFoundError("No setting '{}'".format(name))

    def set_setting(self, name, value):
        """
        Set the value of setting identified by name.

        Required Arguments:
        name -- str name identifying the setting.
        value -- obj value to store. Will be converted to json by this method.

        """
        self._data_storage.set_container_item(Index.SETTINGS_CONTAINER, name, json.dumps(value))

    def get_settings(self, names):
        """
        Return a dict of all settings listed in names. Return format is::
            {
                name: value,
                name: value,
                ...
            }

        Required Arguments:
        names - a list of str names of settings to fetch.

        """
        return {
            k: json.loads(v)
            for k, v in self._data_storage.get_container_items(Index.SETTINGS_CONTAINER, keys=names).iteritems()
            if v
        }

    def add_document(self, frame_size=2, update_index=True, encoding='utf-8', encoding_errors='strict', **fields):
        """
        Add a document to this index.

        We index text breaking it into frames for analysis. The frame_size param controls the size of those frames.
        Setting frame_size to a number < 1 will result in all text being put into one frame or, to put it another way,
        the text not being broken up into frames.

        The fields need to match the schema for this Index otherwise an InvalidDocumentStructure exception will be
        thrown.

        If you are adding a large number of documents in one hit (calling this method repeatedly) then you should
        consider setting update_index to False for those calls and instead calling ``reindex()`` after all those calls.
        This will deliver a significant speed boost to the index process.

        Required Arguments:
        **fields -- the fields and their values for this document. Calling this method will look something like this:
                    writer.add_document(field1=value1, field2=value2).

        Option Arguments:
        frame_size -- the int size of the text frames to break each indexed ``TEXT`` field of this document into.
                      Defaults to 2. A value < 1 will result in only frame being created containing all the text.
        update_index -- A boolean flag indicating whether to update the index after adding this document or not.
        encoding -- this str argument is passed to ``str.decode()`` to decode all text fields.
        encoding_errors -- str argument passed as the ``errors`` argument to ``str.decode()`` when decoding text fields.
                           Defaults to 'strict'. Other options are 'ignore', 'replace'.

        """
        logger.info('Adding document')
        schema_fields = self._schema.items()
        document_id = uuid.uuid4().hex
        sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

        # Build the frames by performing required analysis.
        frames = {}  # Frame data:: frame_id -> {key: value}
        metadata = {}  # Inverted frame metadata:: field_name -> field_value

        # Shell frame includes all non-indexed and categorical fields
        shell_frame = {}
        for field_name, field in schema_fields:
            if (not field.indexed() or field.categorical()) and field.stored() and field_name in fields:
                shell_frame[field_name] = fields[field_name]

        # Tokenize fields that need it
        logger.info('Starting tokenization')
        frame_count = 0
        for field_name, field in schema_fields:

            if field_name not in fields or not field.indexed() or fields[field_name] is None:
                # Skip non-indexed fields or fields with no value supplied for this document
                continue

            if field.categorical():
                # Record categorical values
                for token in field.analyse(fields[field_name]):
                    try:
                        metadata[field_name].append(token.value)
                    except KeyError:
                        metadata[field_name] = [token.value]
            else:
                # Index non-categorical fields
                field_data = fields[field_name]
                expected_types = (str, bytes, unicode)
                if isinstance(field_data, str) or isinstance(field_data, bytes):
                    try:
                        field_data = field_data.decode(encoding, encoding_errors)
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
                        sentences_by_frames = [sentences[i:i+frame_size] for i in xrange(0, len(sentences), frame_size)]
                    else:
                        sentences_by_frames = [[paragraph.value]]
                    for sentence_list in sentences_by_frames:
                        # Build our frames
                        frame_id = "{}-{}".format(document_id, frame_count)
                        frame_count += 1
                        frame = {
                            '_id': frame_id,
                            '_field': field_name,
                            '_positions': {},
                            '_sequence_number': frame_count,
                            '_doc_id': document_id,
                            '_indexed': False,
                        }
                        if field.stored():
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
                                        frame['_positions'][token.value].append(token.index)
                                    except KeyError:
                                        frame['_positions'][token.value] = [token.index]

                        # Build the final frame
                        frame.update(shell_frame)
                        frames[frame_id] = frame

        # If someone wants to store something besides text we need to handle it if we want to be a storage engine.
        # This only applies if we had at least 1 indexed field otherwise the frame can't be retrieved.
        # If there was at least 1 indexed field then there must be metadata!
        if not frames and metadata:
            frame_id = "{}-{}".format(document_id, frame_count)
            frame = {
                '_id': frame_id,
                '_field': None,  # There is no text field
                '_positions': {},
                '_sequence_number': frame_count,
                '_doc_id': document_id,
                '_indexed': False,
            }
            frame.update(shell_frame)
            frames[frame_id] = frame

        # Make sure we store the meta-data on the frame
        for f_id in frames:
            frames[f_id]['_metadata'] = metadata

        logger.info('Tokenization complete. {} frames created.'.format(len(frames)))

        # Store the frames - really easy
        self._data_storage.set_container_items(Index.FRAMES_CONTAINER,
                                               {k: json.dumps(v) for k, v in frames.iteritems()})

        # Update the index data structures if needed.
        if update_index:
            logger.info('Updating index.')
            self.update(frames=frames.values())
        else:
            # Updating the index will update the revision number for us but even if we don't update the index data
            # structures we should still update the revision because we have new docs. Also make sure we set the index
            # to be dirty.
            self.update_revision()
            self.set_clean(False)

        # Finally add the document to storage.
        doc_fields = {'_id': document_id}
        for field_name, field in schema_fields:
            if field.stored() and field_name in fields:
                # Only record stored fields against the document
                doc_fields[field_name] = fields[field_name]
        doc_data = json.dumps(doc_fields).decode(encoding, encoding_errors)
        self._data_storage.set_container_item(Index.DOCUMENTS_CONTAINER, document_id, doc_data)

        return document_id

    def update(self, frames=None):
        """
        Add any frames that have yet to be indexed to the index.

        Please note, updating the index DOES NOT re-run tokenization on the frames. Tokenization is only performed once
        on a document and the output from that tokenization stored against the frame.

        This method is a proxy for `reindex()` called with `update_only` set to True.


        Optional Arguments:
        frames - A list of frames to iterate through checking for un-indexed frames. If None, fetches all frames off the
        index. Defaults to None.

        """
        logger.info('Updating the index.')
        self.reindex(frames=frames, update_only=True)

    def reindex(self, fold_case=False, update_only=False, frames=None, merges=None):
        """
        Re-build or update the index based on the tokenization information stored on the frames. The frames were
        previously generated by adding documents to this index.

        Please note, re-indexing DOES NOT re-run tokenization on the frames. Tokenization is only performed once on a
        document and the output from that tokenization stored against the frame.

        Consider using this method when adding a lot of documents to the index at once. In that case, each call to
        `add_document()` would set `update_index` to False when calling that method and instead call this method after
        adding all the documents (probably with `update_only` set to True). This will deliver a significant speed boost
        to the indexing process in that situation.

        Optional Arguments:
        fold_case -- A boolean flag indicating whether to perform case folding after re-indexing.
        frames - A list of frames to iterate through checking for un-indexed frames. If None, fetches all frames off the
        index. Defaults to None.
        update_only -- A boolean flag indicating whether to perform an update only rather then a full re-index. An upate
        won't remove all existing index structures and overwrite them. Instead, it will only use frames where _indexed
        is set to False and will add to existing index structures.
        merges --  A list of merges to perform after the reindex via ``merge_terms``.

        """
        logger.info('Re-building the index.')
        positions = {}  # Inverted term positions index:: term -> [(start, end,), (star,end,), ...]
        associations = {}  # Inverted term co-occurrence index:: term -> other_term -> count
        frequencies = nltk.probability.FreqDist()  # Inverted term frequency index:: term -> count
        metadata = {}  # Inverted frame metadata:: field_name -> field_value -> [frame1, frame2]
        frames = frames or self.get_frames().values()  # All frames on the index
        frames_to_save = []  # Frames where the indexed paramater needs to be updated

        if update_only:
            frames = filter(lambda f: not f['_indexed'], frames)

        for frame in frames:
            frame_id = frame['_id']

            # Positions & frequencies First
            for term, indices in frame['_positions'].iteritems():
                frequencies.inc(term)
                try:
                    positions[term][frame_id] = positions[term][frame_id] + indices
                except KeyError:
                    try:
                        positions[term][frame_id] = indices
                    except KeyError:
                        positions[term] = {frame_id: indices}
            # Associations next
            for term in frame['_positions']:
                for other_term in frame['_positions']:
                    if term == other_term:
                        continue
                    try:
                        associations[term][other_term] = associations[term].get(other_term, 0) + 1
                    except KeyError:
                        associations[term] = {other_term: 1}
            # Metadata
            if frame['_metadata']:
                for field_name, values in frame['_metadata'].iteritems():
                    for value in values:
                        if value is None:
                            # Skip null values
                            continue
                        try:
                            metadata[field_name][value].append(frame_id)
                        except KeyError:
                            try:
                                metadata[field_name][value] = [frame_id]
                            except KeyError:
                                metadata[field_name] = {value: [frame_id]}
            # Record text field metadata
            field_name = frame['_field']
            if field_name is not None:
                try:
                    metadata[field_name]['_text'].append(frame_id)
                except KeyError:
                    metadata[field_name] = {'_text': [frame_id]}

            if not frame['_indexed']:
                frame['_indexed'] = True
                frames_to_save.append(frame)

        # Save the frames that we need to
        if frames_to_save:
            self._data_storage.set_container_items(Index.FRAMES_CONTAINER,
                                                   {f['_id']: json.dumps(f) for f in frames_to_save})

        # Now serialise and store the various parts
        if update_only:
            # Need to merge existing index with un-indexed frames
            positions_index = {k: json.loads(v) if v else {} for k, v in self._results_storage.get_container_items(
                Index.POSITIONS_CONTAINER, positions.keys()).iteritems()}
            assocs_index = {k: json.loads(v) if v else {} for k, v in self._results_storage.get_container_items(
                Index.ASSOCIATIONS_CONTAINER, associations.keys()).iteritems()}
            frequencies_index = {k: json.loads(v) if v else 0 for k, v in self._results_storage.get_container_items(
                Index.FREQUENCIES_CONTAINER, frequencies.keys()).iteritems()}
            metadata_index = {k: json.loads(v) if v else {} for k, v in self._results_storage.get_container_items(
                Index.METADATA_CONTAINER, metadata.keys()).iteritems()}

            # Positions
            for term, indices in positions.iteritems():
                for frame_id, index in indices.iteritems():
                    try:
                        positions_index[term][frame_id] = positions_index[term][frame_id] + index
                    except KeyError:
                        positions_index[term][frame_id] = index

            # Associations
            for term, value in associations.iteritems():
                for other_term, count in value.iteritems():
                    assocs_index[term][other_term] = assocs_index[term].get(other_term, 0) + count

            # Frequencies
            for key, value in frequencies.iteritems():
                frequencies_index[key] = frequencies_index[key] + value

            # Metadata
            for name, values in metadata.iteritems():
                for value, f_ids in values.iteritems():
                    try:
                        metadata_index[name][value].extend(f_ids)
                    except KeyError:
                        metadata_index[name][value] = f_ids
        else:
            positions_index = positions
            assocs_index = associations
            frequencies_index = dict(frequencies)
            metadata_index = metadata
            # Clear out the existing indexes
            self._results_storage.clear_container(Index.POSITIONS_CONTAINER)
            self._results_storage.clear_container(Index.ASSOCIATIONS_CONTAINER)
            self._results_storage.clear_container(Index.FREQUENCIES_CONTAINER)
            self._results_storage.clear_container(Index.METADATA_CONTAINER)

        # Now do the writing
        # Positions
        for key in positions_index:
            positions_index[key] = json.dumps(positions_index[key])
        self._results_storage.set_container_items(Index.POSITIONS_CONTAINER, positions_index)
        # Associations
        for key in assocs_index:
            assocs_index[key] = json.dumps(assocs_index[key])
        self._results_storage.set_container_items(Index.ASSOCIATIONS_CONTAINER, assocs_index)
        # Frequencies
        for key in frequencies_index:
            frequencies_index[key] = json.dumps(frequencies_index[key])
        self._results_storage.set_container_items(Index.FREQUENCIES_CONTAINER, frequencies_index)
        # Metadata
        for key in metadata_index:
            metadata_index[key] = json.dumps(metadata_index[key])
        self._results_storage.set_container_items(Index.METADATA_CONTAINER, metadata_index)
        logger.info("Re-indexed {} frames.".format(len(frames)))

        if fold_case:
            logger.info('Performing case folding.')
            self.fold_term_case()

        if merges:
            logger.info('Performing manual merges.')
            self.merge_terms(merges)

        # Finally, update index state
        self.update_revision()
        self.set_clean(True)

    def delete_document(self, d_id, update_index=True):
        """
        Delete the document with given id.

        Raises a DocumentNotFound exception if the id doesn't match any document.

        This method needs to update the index to reflect that the document has been deleted. This is an optional step.
        However, if it isn't performed, you may see odd results for things like `get_frequencies()`.

        Required Arguments:
        id -- the string id of the document to delete.

        Optional Arguments:
        update_index -- a bool flag indicating whether to update the index or not. If set to ``False`` the index won't
                        reflect the fact that the document has been deleted until `reindex()` has been run.

        """
        try:
            doc = self._data_storage.get_container_item(Index.DOCUMENTS_CONTAINER, d_id)
        except KeyError:
            raise DocumentNotFoundError("No such document {}".format(d_id))
        frames = {
            k: json.loads(v)
            for k, v in self._data_storage.get_container_items(Index.FRAMES_CONTAINER).iteritems()
        }
        frames_to_delete = []
        for f_id, frame in frames.iteritems():
            if frame['_doc_id'] == d_id:
                frames_to_delete.append(f_id)
        self._data_storage.delete_container_items(Index.FRAMES_CONTAINER, frames_to_delete)
        if update_index:
            self.reindex(fold_case=False)
        else:
            # We have changes!
            self.update_revision()
            self.set_clean(False)
        self._data_storage.delete_container_item(Index.DOCUMENTS_CONTAINER, d_id)

    def fold_term_case(self, merge_threshold=0.7):
        """
        Perform case folding on the index, merging words into names and vice-versa depending on the specified threshold.

        Optional Arguments:
        merge_threshold -- A float used to test when to merge two variants. When the ratio between word and name version
                           of term falls below this threshold the merge is carried out.

        """
        count = 0
        # Pre-fetch indexes and pass them around to save I/O
        frequencies_index = {
            k: json.loads(v) if v else 0
            for k, v in self._results_storage.get_container_items(Index.FREQUENCIES_CONTAINER).iteritems()
        }
        associations_index = {
            k: json.loads(v) if v else {}
            for k, v in self._results_storage.get_container_items(Index.ASSOCIATIONS_CONTAINER).iteritems()
        }
        positions_index = {
            k: json.loads(v) if v else {}
            for k, v in self._results_storage.get_container_items(Index.POSITIONS_CONTAINER).iteritems()
        }
        frames = {
            k: json.loads(v)
            for k, v in self._data_storage.get_container_items(Index.FRAMES_CONTAINER).iteritems()
        }
        for w, freq in frequencies_index.items():
            if w.islower() and w.title() in frequencies_index:
                freq_name = frequencies_index[w.title()]
                if freq / freq_name < merge_threshold:
                    # Merge into name
                    logger.debug('Merging {} into {}'.format(w, w.title()))
                    self._merge_terms(w, w.title(), associations_index, positions_index, frequencies_index, frames)
                    count += 1
                elif freq_name / freq < merge_threshold:
                    # Merge into word
                    logger.debug('Merging {} into {}'.format(w.title(), w))
                    self._merge_terms(w.title(), w, associations_index, positions_index, frequencies_index, frames)
                    count += 1

        # Update stored data structures
        self._results_storage.clear(Index.ASSOCIATIONS_CONTAINER)
        self._results_storage.set_container_items(Index.ASSOCIATIONS_CONTAINER,
                                                  {k: json.dumps(v) for k, v in associations_index.iteritems()})
        self._results_storage.clear(Index.POSITIONS_CONTAINER)
        self._results_storage.set_container_items(Index.POSITIONS_CONTAINER,
                                                  {k: json.dumps(v) for k, v in positions_index.iteritems()})
        self._results_storage.clear(Index.FREQUENCIES_CONTAINER)
        self._results_storage.set_container_items(Index.FREQUENCIES_CONTAINER,
                                                  {k: json.dumps(v) for k, v in frequencies_index.iteritems()})
        self._data_storage.set_container_items(Index.FRAMES_CONTAINER,
                                               {f_id: json.dumps(d) for f_id, d in frames.iteritems()})
        logger.info("Merged {} terms during case folding.".format(count))

    def merge_terms(self, merges):
        """
        Merge the terms in `merges`.

        Required Arguments:
        merges - A list of str tuples of the format `(old_term, new_term,)`. If new_term is '' then old_term is removed.
                 N-grams can be specified by suppling a str tuple instead of str for the old term. For example:
                    (('hot', 'dog'), 'hot dog')
                 The n-gram case does not allow empty values for new_term.

        """
        count = 0
        # Pre-fetch indexes and pass them around to save I/O
        frequencies_index = {
            k: json.loads(v) if v else 0
            for k, v in self._results_storage.get_container_items(Index.FREQUENCIES_CONTAINER).iteritems()
        }
        associations_index = {
            k: json.loads(v) if v else {}
            for k, v in self._results_storage.get_container_items(Index.ASSOCIATIONS_CONTAINER).iteritems()
        }
        positions_index = {
            k: json.loads(v) if v else {}
            for k, v in self._results_storage.get_container_items(Index.POSITIONS_CONTAINER).iteritems()
        }
        frames = {
            k: json.loads(v)
            for k, v in self._data_storage.get_container_items(Index.FRAMES_CONTAINER).iteritems()
        }
        for old_term, new_term in merges:
            logger.debug('Merging {} into {}'.format(old_term, new_term))
            try:
                if isinstance(old_term, basestring):
                    self._merge_terms(old_term, new_term, associations_index, positions_index, frequencies_index,
                                      frames)
                else:
                    self._merge_terms_into_ngram(old_term, new_term, associations_index, positions_index,
                                                 frequencies_index, frames)
                count += 1
            except TermNotFoundError:
                logger.exception('One of the terms doesn\'t exist in the index!')

        # Update indexes
        self._results_storage.clear(Index.ASSOCIATIONS_CONTAINER)
        self._results_storage.set_container_items(Index.ASSOCIATIONS_CONTAINER,
                                                  {k: json.dumps(v) for k, v in associations_index.iteritems()})
        self._results_storage.clear(Index.POSITIONS_CONTAINER)
        self._results_storage.set_container_items(Index.POSITIONS_CONTAINER,
                                                  {k: json.dumps(v) for k, v in positions_index.iteritems()})
        self._results_storage.clear(Index.FREQUENCIES_CONTAINER)
        self._results_storage.set_container_items(Index.FREQUENCIES_CONTAINER,
                                                  {k: json.dumps(v) for k, v in frequencies_index.iteritems()})
        self._data_storage.set_container_items(Index.FRAMES_CONTAINER,
                                               {f_id: json.dumps(d) for f_id, d in frames.iteritems()})
        logger.info("Merged {} terms during manual merge.".format(count))

    def _merge_terms(self, old_term, new_term, associations, positions, frequencies, frames):
        """
        Merge old_term into new_term. If new_term is a falsey value, old_term is deleted. Updates all the passed index
        structures to reflect the change.

        Raises TermNotFoundError if the term does not exist in the index.

        """
        try:
            old_positions = positions[old_term]
            new_positions = positions[new_term] if new_term in positions else {}
        except KeyError:
            raise TermNotFoundError("Term '{}' does not exist.".format(old_term))

        # Clear global associations for old term
        if old_term in associations:
            for term in associations[old_term]:
                del associations[term][old_term]
            del associations[old_term]

        # Update term positions globally and positions and associations in frames
        for frame_id, frame_positions in old_positions.iteritems():

            # Remove traces of old term from frame data
            del frames[frame_id]['_positions'][old_term]

            # Update global positions
            if new_term:
                try:
                    new_positions[frame_id].extend(frame_positions)
                except KeyError:
                    # New term had not been recorded in this frame
                    new_positions[frame_id] = frame_positions

                # Update global associations
                for term in frames[frame_id]['_positions']:
                    if new_term not in frames[frame_id]['_positions'] and old_term != term and term != new_term:
                        # Only count frames that do not contain the new spelling as those frames have already
                        # been counted.
                        # We need to change these separately because there is a very slim chance that new_term isn't
                        # in the associations index yet while term will always be present.
                        # Term first
                        try:
                            associations[term][new_term] += 1
                        except KeyError:
                            associations[term][new_term] = 1
                        # Now new_term
                        try:
                            associations[new_term][term] += 1
                        except KeyError:
                            try:
                                associations[new_term][term] = 1
                            except KeyError:
                                associations[new_term] = {term: 1}

                # Update frame positions
                try:
                    frames[frame_id]['_positions'][new_term].extend(frame_positions)
                except KeyError:
                    # New term had not been recorded in this frame
                    frames[frame_id]['_positions'][new_term] = frame_positions

        # Update positions and frequency index
        if new_term:
            positions[new_term] = new_positions
            frequencies[new_term] = len(new_positions)

        # Finally, purge.
        del positions[old_term]
        del frequencies[old_term]

    def _merge_terms_into_ngram(self, ngram_terms, new_term, associations, positions, frequencies, frames, MAX_GAP=2):
        """
        Merge n-gram term sequences into new_term and update all the passed index structures to reflect the change.

        This method cannot be used to delete terms, so a falsey value for new_term will produce an exception. Also,
        raises TermNotFoundError if the n-gram does not exist in the index.

        MAX_GAP specifies the maximum number of characters allowed between n-gram terms.

        """
        if not new_term:
            raise Exception('A non-empty value must be specified to represent the n-gram')

        # First we need to find the positions of all matching n-grams in the index.
        # We start with the positions of the first term in the n-gram...
        try:
            ngram_positions = positions[ngram_terms[0]].copy()
        except KeyError:
            raise TermNotFoundError("N-gram '{}' does not exist.".format(ngram_terms))

        # ..then loop through remaining n-gram terms, updating and
        # restricting the recorded n-gram positions in the process
        for next_term in ngram_terms[1:]:
            try:
                next_positions = positions[next_term]
            except KeyError:
                raise TermNotFoundError("N-gram '{}' does not exist.".format(ngram_terms))

            consumed_start_positions = {}
            matched_frames = []

            # Match the next n-gram term, for each frame it occurs in
            for f_id, f_next_positions in next_positions.iteritems():

                if f_id not in ngram_positions:
                    # No valid n-gram sequences in this frame, skip to next one
                    continue

                # Look for a proximal match against each recorded partial n-gram sequence
                joined_positions = []
                for curr_pos in ngram_positions[f_id]:
                    if curr_pos[0] in consumed_start_positions.get(f_id, []):
                        # Already recorded a match against this partial sequence so skip it
                        continue
                    # Loop over positions of the next n-gram term
                    for next_pos in f_next_positions:
                        if curr_pos[0] < next_pos[0] and next_pos[0] - curr_pos[1] <= MAX_GAP:
                            # Next term coincides with current partial n-gram sequence, record the match and move on
                            new_pos = [curr_pos[0], next_pos[1]]
                            try:
                                joined_positions.append(new_pos)
                                consumed_start_positions[f_id].append(next_pos[0])
                            except:
                                joined_positions = [new_pos]
                                consumed_start_positions[f_id] = [next_pos[0]]
                            break  # Break out to move to next partial n-gram sequence

                # Update positions
                ngram_positions[f_id] = joined_positions
                if len(joined_positions) > 0:
                    matched_frames.append(f_id)

        # Only include frames that had an n-gram match
        ngram_positions = {
            f_id: f_positions for f_id, f_positions in ngram_positions.iteritems()
            if f_id in matched_frames
        }

        # Next we have to update frame and global information in light of the matched n-grams
        for f_id, f_ngram_positions in ngram_positions.iteritems():

            f_positions = frames[f_id]['_positions']

            # Update some information for terms consumed by n-gram matches
            unique_terms = set(ngram_terms)  # Don't duplicate the process for n-grams with repeating terms
            for term in unique_terms:

                # Need to update the positions to reflect only the individual term occurrences
                updated_positions = []
                for term_pos in f_positions[term]:
                    ngram_match = False
                    for ngram_pos in f_ngram_positions:
                        if (ngram_pos[0] <= term_pos[0] <= ngram_pos[1] or
                                ngram_pos[0] <= term_pos[1] <= ngram_pos[1]):
                            # Term position coincides with n-gram position
                            ngram_match = True
                            break  # Break out to check next position of term
                    if not ngram_match:
                        # Record the indivudal term occurrence
                        updated_positions.append(term_pos)
                f_positions[term] = updated_positions

                if len(f_positions[term]) > 0:
                    # We still had individual occurrences of term in this frame,
                    # so set global positions based on updated frame positions.
                    positions[term][f_id] = f_positions[term]
                else:
                    # No occurrences of the term in this frame were part of an n-gram sequence,
                    # so adjust frequecies, associations and positions accordingly
                    frequencies[term] -= 1
                    del positions[term][f_id]
                    del f_positions[term]
                    for other_term in filter(lambda t: t != new_term and t in f_positions, associations[term]):
                        associations[term][other_term] -= 1
                        associations[other_term][term] -= 1
                        if associations[term][other_term] == 0:
                            # Remove entry if associations are 0
                            del associations[term][other_term]
                            del associations[other_term][term]

            # Record associations for the new term
            for other_term in f_positions:
                try:
                    associations[new_term][other_term] = associations[new_term].get(other_term, 0) + 1
                except KeyError:
                    associations[new_term] = {other_term: 1}
                try:
                    associations[other_term][new_term] += 1
                except KeyError:
                    associations[other_term][new_term] = 1

            # Update frame positions for new term
            f_positions[new_term] = f_ngram_positions

        # Last global updates for new term
        frequencies[new_term] = len(ngram_positions)
        positions[new_term] = ngram_positions

    def run_plugin(self, cls, **args):
        """
        Runs an ``plugin.AnalyticsPlugin`` on this index. Creates an instance of the plugin by passing it this object
        before calling its run method and saving the result into container(s) prefixed with the plugins name.

        Any keyword arguments passed to this method in args are passed onto the run method of the plugin.

        Required Arguments:
        cls -- a python class that extends ``plugin.AnalyticsPlugin``

        Optional Arguments:
        **args -- keywords args that are passed onto the run method of an instance of cls.

        """
        plugin = cls(self)
        logger.info('Running {} plugin.'.format(plugin.get_name()))
        result = plugin.run(**args)

        for container, value in result.iteritems():
            # Max sure the container exists and is clear
            container_id = Index._plugin_container_name(plugin.get_name(), container)
            try:
                self._results_storage.add_container(container_id)
            except DuplicateContainerError:
                self._results_storage.clear_container(container_id)

            # Make sure items are seralised
            for key, data in value.items():
                value[key] = json.dumps(value[key])

            # Store items
            self._results_storage.set_container_items(container_id, value)

        return plugin

    def get_plugin_data(self, plugin, container, keys=None):
        """
        Returns a container identified by container for the plugin instance.

        Required Arguments:
        plugin -- an instance of ``plugin.AnalyticsPlugin``.
        container -- The str name of the container.

        Optional Arguments:
        keys -- the list of keys to fetch. Defaults to None meaning all items are fetched.

        """
        return self._results_storage.get_container_items(Index._plugin_container_name(plugin.get_name(), container),
                                                         keys)

    @staticmethod
    def _plugin_container_name(plugin, container):
        """
        Naming convention for plugin containers.

        """
        return '{}__{}'.format(plugin, container)


def find_bi_gram_words(frames, min_bi_gram_freq=3, min_bi_gram_coverage=0.65):
    """
    This function finds bi-gram words from the specified ``Frame``s iterable which pass certain criteria.

    This function uses a ``PotentialBiGramAnalyser`` to identify potential bi-grams. Names and stopwords are not
    considered for bi-grams. Only bi-grams that have a certain frequency and coverage are considered.

    Required Arguments:
    frames -- An iterable of ``Frame`` objects for textual data

    Optional Arguments:
    min_bi_gram_freq -- A int representing the minimum frequency cutoff for each bi_gram
    min_bi_gram_coverage -- A float representing the cutoff ratio for bi_gram frequency over individual word frequencies

    Returns a list of bi-gram strings that pass the criteria.

    """
    logger.info("Identifying n-grams")

    # Generate a table of candidate bigrams
    candidate_bi_grams = nltk.probability.FreqDist()
    uni_gram_frequencies = nltk.probability.FreqDist()
    bi_gram_analyser = PotentialBiGramAnalyser()
    for frame in frames:
        for sentence in frame.sentences:
            terms_seen = set()
            for token_list in bi_gram_analyser.analyse(sentence):
                # Using a special filter that returns list of tokens. List of 1 means no bi-grams.
                if len(token_list) > 1:  # We have a bi-gram people!
                    candidate_bi_grams.inc("{} {}".format(token_list[0].value, token_list[1].value))
                for t in token_list:  # Keep a list of terms we have seen so we can record freqs later.
                    if not t.stopped:  # Naughty stopwords!
                        terms_seen.add(t.value)
            for term in terms_seen:
                uni_gram_frequencies.inc(term)

    # Filter and sort by frequency-decreasing
    candidate_bi_gram_list = filter(lambda (k, v): v > min_bi_gram_freq, candidate_bi_grams.iteritems())
    candidate_bi_gram_list = filter(lambda (k, v): v / uni_gram_frequencies[k.split(" ")[0]] > min_bi_gram_coverage
                                    and v / uni_gram_frequencies[k.split(" ")[1]] > min_bi_gram_coverage,
                                    candidate_bi_gram_list)
    logger.info("Identified {} n-grams.".format(len(candidate_bi_gram_list)))

    return [b[0] for b in candidate_bi_gram_list]


class EmptyCompositeQueryError(Exception):
    """Composite query has no results."""


class DerivedIndex(Index):
    """
    Subclass of ``Index`` that allows for a derived index to be created by the filtering/composition of any number
    of existing indices.

    It does not support any methods that involve documents.

    Required Arguments:
    schema -- a ``schema.Schema`` instance that covers all fields in this index.
    data_storage -- a ``data.storage.Storage`` instance that will be used to write and read document, frame and config
    data.
    results_storage -- a ``data.storage.Storage`` instance that will be used to write and read processing results.
    frames -- a list of frames to construct the index from.

    """
    def __init__(self, schema, data_storage, results_storage, frames):
        self._schema = schema
        self._data_storage = data_storage
        self._results_storage = results_storage

        # Store schema
        self._data_storage.set_container_item(Index.SETTINGS_CONTAINER, Index.SETTINGS_SCHEMA, schema.dumps())
        # Store frames and build the index
        self._data_storage.set_container_items(Index.FRAMES_CONTAINER,
                                               {k: json.dumps(v) for k, v in frames.iteritems()})
        self.reindex()
        data_storage.set_container_item(Index.INFO_CONTAINER, 'derived', json.dumps(True))

    @staticmethod
    def create_from_composite_query(index_queries, path=None, storage_cls=SqliteMemoryStorage, **args):
        """
        Create a new ``DerivedIndex`` from an arbitrary number of index queries. Stores the fact that this index is a
        derived index in `Index.INFO_CONTAINER`.

        Required Arguments:
        index_queries -- A list of tuples in the form of (index, query_string, [text_field]).

        Optional Arguments:
        path -- path to store index data under. MUST be specified if persistent storage is used.
        storage_cls -- the class to instantiate for the storage object. Defaults to ``SqliteMemoryStorage``.
        The create() method will be called on this instance.
        **args -- any keyword arguments that need to be passed onto the storage instance.

        """
        fields = {}
        frames = {}
        for index_query in index_queries:
            index = index_query[0]
            query = index_query[1]
            text_field = index_query[2] if len(index_query) > 2 else None
            frame_ids = list(index.searcher().filter(query, text_field=text_field))
            if len(frame_ids) > 0:
                frames.update(index.get_frames(frame_ids=frame_ids))
                fields.update(index.get_schema().items())

        if len(frames) == 0:
            raise EmptyCompositeQueryError("Composite query did not yield any frames")

        schema = Schema(**fields)
        data_storage = storage_cls.create(Index.DATA_STORAGE, path=path, acid=True, containers=[
            Index.DOCUMENTS_CONTAINER,
            Index.FRAMES_CONTAINER,
            Index.SETTINGS_CONTAINER,
            Index.INFO_CONTAINER,
        ], **args)
        results_storage = storage_cls.create(Index.RESULTS_STORAGE, path=path, acid=False, containers=[
            Index.POSITIONS_CONTAINER,
            Index.ASSOCIATIONS_CONTAINER,
            Index.FREQUENCIES_CONTAINER,
            Index.METADATA_CONTAINER
        ], **args)

        return DerivedIndex(schema, data_storage, results_storage, frames)

    def add_document(self, frame_size=None, fold_case=None, update_index=None, encoding=None, **fields):
        DerivedIndex.documents_not_supported_error()

    def delete_document(self, d_id, update_index=True):
        DerivedIndex.documents_not_supported_error()

    def get_document(self, d_id):
        DerivedIndex.documents_not_supported_error()

    def get_document_count(self):
        DerivedIndex.documents_not_supported_error()

    @staticmethod
    def documents_not_supported_error():
        raise NotImplementedError("Documents not supported by DerivedIndex")
