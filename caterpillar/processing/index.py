# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""
An index represents a collection of documents and associated information about those documents. When a document is added
to an index using an :class:`.IndexWriter`, some or all of its fields will be analysed
(see :mod:`caterpillar.processing.schema`) and information about those fields stored in various sub-indexes.
Caterpillar stores a number of sub-indexes:

* The frequencies index::
    {
        "term": count,
        "term": count,
        ...
    }
* The positions index (an inverted text index)::
    {
        "term": {
            "frame_id": [(start, end), (start, end)],
            ...
        },
        ...
    }
* The associations index::
    {
        term: {
            other_term: count,
            ...
        },
        ...
    }

Documents can be added to an index using an :class:`.IndexWriter`. Data can be read from an index using
:class:`.IndexReader`. There can only ever be one ``IndexWriter`` active per index. There can be an unlimited number of
``IndexReader``s active per index.

The type of index stored by caterpillar is different from those stored by regular information retrieval libraries (like
Lucene for example). Caterpillar is designed for text analytics as well as information retrieval. One side affect of
this is that caterpillar breaks documents down into *frames*. Breaking documents down into smaller parts (or context
blocks) enables users to implement their own statistical methods for analysing text. Frames are a configurable
component. See :class:`.IndexWriter` for more information.

Here is a quick example:

    >>> from caterpillar.processing import index
    >>> from caterpillar.processing import schema
    >>> from caterpillar.storage.sqlite import SqliteStorage
    >>> config = index.IndexConfig(SqliteStorage, schema.Schema(text=schema.TEXT))
    >>> with index.IndexWriter('/tmp/test_index', config) as writer:
    ...     writer.add_document(text="This is my text")...
    '935ed96520ab44879a6e76195a9d7046'
    >>> with index.IndexReader('/tmp/test_index') as reader:
    ...     reader.get_document_count()
    ...
    1

"""

from __future__ import absolute_import, division, unicode_literals

import logging
import os
import cPickle
import random
import sys
import ujson as json
import uuid

import nltk

from .analysis.analyse import PotentialBiGramAnalyser
from .analysis.tokenize import ParagraphTokenizer, Token
from caterpillar import VERSION
from caterpillar.locking import PIDLockFile, LockTimeout, AlreadyLocked
from caterpillar.searching import TfidfScorer, IndexSearcher
from caterpillar.storage import StorageNotFoundError


logger = logging.getLogger(__name__)


class CaterpillarIndexError(Exception):
    """Common base class for index errors."""


class DocumentNotFoundError(CaterpillarIndexError):
    """No document by that name exists."""


class SettingNotFoundError(CaterpillarIndexError):
    """No setting by that name exists."""


class IndexNotFoundError(CaterpillarIndexError):
    """No index exists at specified location."""


class TermNotFoundError(CaterpillarIndexError):
    """Term doesn't exist in index."""


class IndexWriteLockedError(CaterpillarIndexError):
    """There is already an existing writer for this index."""


class IndexConfig(object):
    """
    Stores configuration information about an index.

    This object is a core part of any index. It is serialised and stored with every index so that an index can be
    opened. It tells an :class:`IndexWriter` and :class:`IndexReader` what type of storage class to use via
    ``storage_cls`` (must be a subclass of :class:`Storage <caterpillar.storage.Storage>`) and structure of the index
    via ``schema`` (an instance of :class:`Schema <caterpillar.processing.schema.Schema>`).

    In the interest of future proofing this object, it will also store a ``version`` number with itself so that
    older/new version have the best possible chance at opening indexes.

    This class might be extended later to store other things.

    """

    def __init__(self, storage_cls, schema):
        self._storage_cls = storage_cls
        self._schema = schema
        self._version = VERSION

    @property
    def storage_cls(self):
        return self._storage_cls

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema

    @property
    def version(self):
        return self._version

    @staticmethod
    def loads(data):
        """
        Create an instance of this class from a string generated via :meth:`.dumps`.

        Raises :exc:`ValueError` if the ``data`` (str) can't be parsed into an instance of ``IndexConfig``.

        """
        try:
            instance = cPickle.loads(data)
            if not isinstance(instance, IndexConfig):
                raise ValueError('The passed data couldn\'t be parsed.')
        except Exception:
            raise ValueError('The passed data couldn\'t be parsed.')
        else:
            return instance

    def dumps(self):
        """Dump this instance as a string for serialization."""
        return cPickle.dumps(self)


class IndexWriter(object):

    """
    Write to an existing index or create a new index and write to it.

    An instance of an ``IndexWriter`` represents a transaction. To begin the transaction, you need to call
    :meth:`.begin` on this writer. There can only be one active IndexWriter transaction per index. This is enforced
    via a write lock on the index. When you begin the write transaction the IndexWriter instance tries to acquire the
    write lock. By default it will block indefinitely until it gets the write lock but this can be overridden using the
    ``timeout`` argument to `begin()`. If `begin()` times-out when trying to get a lock, then
    :exc:`IndexWriteLockedError` is raised.

    Writes to an index are internally buffered until they reach :const:`IndexWriter.RAM_BUFFER_MB` when `flush()` is
    called. Alternatively a caller is free to call flush whenever they like. Calling flush will take any in-memory
    writes recorded by this class and write them to the underlying storage object using the methods it provides.

    .. note::
        The behaviour of `flush()` depends on the underlying :class:`Storage <chrysalis.data.storage.Storage>`
        implementation used. Some implementations might just record the writes in memory. Consult the specific storage
        type for more information.

    Once you have performed all the writes/deletes you like you need to call :meth:`.commit` to finalise the
    transaction. Alternatively, if there was a problem during your transaction, you can call :meth:`.rollback` instead
    to revert any changes you made using this writer. **IMPORTANT** - Finally, you need to call :meth:`.close` to
    release the lock.

    Using IndexWriter this way should look something like this:

        >>> writer = IndexWriter('/some/path/to/an/index')
        >>> try:
        ...     writer.begin(timeout=2)  # timeout in 2 seconds
        ...     # Do stuff, like add_document(), flush() etc...
        ...     writer.commit()  # Write the changes (calls flush)...
        ... except IndexWriteLockedError:
        ...     # Do something else, maybe try again
        ... except SomeOtherException:
        ...     writer.rollback()  # Error occurred, undo our changes
        ... finally:
        ...     writer.close()  # Release lock

    This class is also a context manager and so can be used via the with statement. **HOWEVER**, be aware that using
    this class in a context manager will block indefinitely until the lock becomes available. Using the context manager
    has the added benefit of calling ``commit()``/``rollback()`` (if an exception breaks the context) and ``close()`` \
    for you automatically::

        >>> writer = IndexWriter('/some/path/to/a/index')
        >>> with writer:
        ...     add_document(field="value")

    Again, be warned that this will block until the write lock becomes available!

    Finally, pay particular attention to the ``frame_size`` arguments of :meth:`.add_document`. This determines the size
    of the frames the document will be broken up into.

    """
    # Static storage names
    DOCUMENTS_CONTAINER = "documents"
    SETTINGS_CONTAINER = "settings"
    INFO_CONTAINER = "info"
    METADATA_CONTAINER = "metadata"

    # One frame container per indexed text field
    FRAMES_CONTAINER = "frames_{}"
    FREQUENCIES_CONTAINER = "frequencies_{}"
    POSITIONS_CONTAINER = "positions_{}"
    ASSOCIATIONS_CONTAINER = "associations_{}"

    # How much data to buffer before a flush
    RAM_BUFFER_SIZE = 100 * 1024 * 1024  # 100 MB

    # Where is the config?
    CONFIG_FILE = "index.config"

    def __init__(self, path, config=None):
        """
        Open an existing index for writing or create a new index for writing.

        If ``path`` (str) doesn't exist and ``config`` is not None, then a new index will created when :meth:`begin` is
        called (after the lock is acquired. Otherwise, :exc:`IndexNotFoundError` is raised.

        If present, ``config`` (IndexConfig) must be an instance of :class:`IndexConfig`.

        """
        self._path = path
        if not config and not os.path.exists(path):
            # Index path doesn't exist and no schema passed
            raise IndexNotFoundError('No index exists at {}'.format(path))
        elif config and not os.path.exists(path):
            # Index doesn't exist. Delay creating until we have the lock in begin().
            self.__config = config
            self.__schema = config.schema
            self.__storage = None
        else:
            # Fetch the config
            with open(os.path.join(path, IndexWriter.CONFIG_FILE), 'r') as f:
                self.__config = IndexConfig.loads(f.read())
            self.__storage = self.__config.storage_cls(path, create=False)
            self.__schema = self.__config.schema
            self.__lock = None  # Should declare in __init__ and not outside.
        self.__committed = False
        # Internal index buffers we will update when flush() is called.
        self.__new_frames = {}
        self.__new_documents = {}
        self.__rm_frames = {}
        self.__rm_documents = set()

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()

    def begin(self, timeout=None):
        """
        Acquire the write lock and begin the transaction.

        If this index has yet to be created, create it (folder and storage). If ``timeout``(int) is omitted (or None),
        wait forever trying to lock the file. If ``timeout`` > 0, try to acquire the lock for that many seconds. If the
        lock period expires and the lock hasn't been acquired raise :exc:`IndexWriteLockedError`. If timeout <= 0,
        raise :exc:`IndexWriteLockedError` immediately if the lock can't be acquired.

        """
        created = os.path.exists(self._path)
        if not created:
            os.makedirs(self._path)
        self.__lock = PIDLockFile(os.path.join(self._path, 'writer'))
        try:
            self.__lock.acquire(timeout=timeout)
        except (AlreadyLocked, LockTimeout):
            raise IndexWriteLockedError('Index {} is locked for writing'.format(self._path))
        else:
            logger.debug("Index write lock acquired for {}".format(self._path))
            if not created:
                # Store config
                with open(os.path.join(self._path, IndexWriter.CONFIG_FILE), 'w') as f:
                    f.write(self.__config.dumps())
                # Initialize storage
                storage = self.__config.storage_cls(self._path, create=True)
                # Need to create the containers
                storage.begin()
                # Universal Containers
                storage.add_container(IndexWriter.DOCUMENTS_CONTAINER)
                storage.add_container(IndexWriter.SETTINGS_CONTAINER)
                storage.add_container(IndexWriter.INFO_CONTAINER)
                storage.add_container(IndexWriter.METADATA_CONTAINER)

                text_fields = self.__schema.get_indexed_text_fields()
                # Set of containers for each indexed text field
                for field_name in text_fields:
                    storage.add_container(IndexWriter.POSITIONS_CONTAINER.format(field_name))
                    storage.add_container(IndexWriter.ASSOCIATIONS_CONTAINER.format(field_name))
                    storage.add_container(IndexWriter.FRAMES_CONTAINER.format(field_name))
                    storage.add_container(IndexWriter.FREQUENCIES_CONTAINER.format(field_name))

                # Catch all container for surrogate frames generated in the case of no text fields.
                # This is a workaround to the limitation of the current index structure which allows
                # searching only by frame and not by documents.
                storage.add_container(IndexWriter.FRAMES_CONTAINER.format(''))
                # Index settings
                storage.set_container_item(IndexWriter.INFO_CONTAINER, 'derived', json.dumps(False))
                # Revision
                storage.set_container_item(IndexWriter.INFO_CONTAINER, 'revision',
                                           json.dumps(random.SystemRandom().randint(0, 10**10)))
                storage.commit()
            if not self.__storage:
                # This is a create or the index was created after this writer was opened.
                self.__storage = self.__config.storage_cls(self._path, create=False)
            self.__storage.begin()

    def flush(self):
        """
        Flush the internal buffers to the underlying storage implementation.

        This method iterates through the frames that have been buffered by this writer and creates an internal index
        of them. Then, it merges that index with the existing index already stored via storage. Finally, it also flushes
        the internal frames and documents buffers to storage before clearing them. This includes both added documents
        and deleted documents.

        .. warning::
            Even thought this method flushes all internal buffers to the underlying storage implementation, this
            does NOT constitute writing your changes! To actually have your changes persisted by the underlying storage
            implementation, you NEED to call :meth:`.commit`! Nothing is final until commit is called.

        """
        logger.debug("Flushing the index writer.")

        def index_frames(frames):
            """Index a set of frames. This includes building the index structure for the frames."""
            fields = self.__schema.get_indexed_text_fields()

            # Inverted term positions index:: field_name --> {term -> [(start, end,), (star,end,), ...]}
            positions = {field_name: {} for field_name in fields}
            # Inverted term co-occurrence index:: field_name --> {term -> other_term -> count
            associations = {field_name: {} for field_name in fields}
            # Inverted term frequency index:: field_name --> {term -> count
            frequencies = {field_name: nltk.probability.FreqDist() for field_name in fields}
            # Inverted frame metadata:: field_name -> {field_value -> [frame1, frame2]}
            metadata = {field_name: {} for field_name in fields}

            # If there are no new frames, return with empty structures
            if len(frames.keys()) == 0:
                return positions, associations, frequencies, metadata

            for text_field_name, field_frames in frames.iteritems():
                for frame_id, frame in field_frames.iteritems():

                    # Positions & frequencies First
                    for term, indices in frame['_positions'].iteritems():
                        frequencies[text_field_name].inc(term)
                        try:
                            positions[text_field_name][term][frame_id] = positions[term][frame_id] + indices
                        except KeyError:
                            try:
                                positions[text_field_name][term][frame_id] = indices
                            except KeyError:
                                positions[text_field_name][term] = {frame_id: indices}

                    # Associations next
                    for term in frame['_positions']:
                        for other_term in frame['_positions']:
                            if term == other_term:
                                continue
                            try:
                                associations[text_field_name][term][other_term] = \
                                    associations[text_field_name][term].get(other_term, 0) + 1
                            except KeyError:
                                associations[text_field_name][term] = {other_term: 1}

                    # Metadata
                    if frame['_metadata']:
                        for metadata_field_name, values in frame['_metadata'].iteritems():
                            for value in values:
                                if value is None:
                                    # Skip null values
                                    continue
                                try:
                                    metadata[metadata_field_name][value].append(frame_id)
                                except KeyError:
                                    try:
                                        metadata[metadata_field_name][value] = [frame_id]
                                    except KeyError:
                                        metadata[metadata_field_name] = {value: [frame_id]}

                    # Record text field metadata
                    field_name = frame['_field']
                    if field_name is not None:
                        try:
                            metadata[field_name]['_text'].append(frame_id)
                        except KeyError:
                            metadata[field_name] = {'_text': [frame_id]}

            return positions, associations, frequencies, metadata

        text_fields = self.__schema.get_indexed_text_fields()

        # Generate the index content of the frames to be removed, to exclude from the merging set
        rm_frames = {}

        # Remember to include the '' field for metadata only surrogate frames.
        for field in self.__rm_frames.keys():
            rm_frames[field] = {
                k: json.loads(v) if v else {}
                for k, v in self.__storage.get_container_items(
                    IndexWriter.FRAMES_CONTAINER.format(field), keys=self.__rm_frames[field]
                )
            }

        new_positions, new_associations, new_frequencies, new_metadata = index_frames(self.__new_frames)
        rm_positions, rm_associations, rm_frequencies, rm_metadata = index_frames(rm_frames)

        # Load on disk representations of the index, for merging with new and deleted frames.
        positions_index = {}
        assocs_index = {}
        frequencies_index = {}

        for text_field in text_fields:
            positions_index[text_field] = {
                k: json.loads(v) if v else {}
                for k, v in self.__storage.get_container_items(
                    IndexWriter.POSITIONS_CONTAINER.format(text_field),
                    keys=new_positions[text_field].viewkeys() | rm_positions[text_field].viewkeys()
                )
            }

            assocs_index[text_field] = {
                k: json.loads(v) if v else {}
                for k, v in self.__storage.get_container_items(
                    IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field),
                    keys=new_positions[text_field].viewkeys() | rm_positions[text_field].viewkeys()
                )
            }

            frequencies_index[text_field] = {
                k: json.loads(v) if v else 0
                for k, v in self.__storage.get_container_items(
                    IndexWriter.FREQUENCIES_CONTAINER.format(text_field),
                    keys=new_positions[text_field].viewkeys() |
                    rm_positions[text_field].viewkeys()
                )
            }

        metadata_index = {
            k: json.loads(v) if v else {}
            for k, v in self.__storage.get_container_items(
                IndexWriter.METADATA_CONTAINER, new_metadata.viewkeys() | rm_metadata.viewkeys()
            )
        }

        # Keys to remove from each index
        delete_positions_keys = set()
        delete_assoc_keys = set()
        delete_frequencies_keys = set()
        delete_metadata_keys = set()

        # Positions
        for text_field in text_fields:
            for term, indices in new_positions[text_field].iteritems():

                for frame_id, index in indices.iteritems():
                    try:
                        positions_index[text_field][term][frame_id] = \
                            positions_index[text_field][term][frame_id] + index

                    except KeyError:
                        positions_index[text_field][term][frame_id] = index

            for term, indices in rm_positions[text_field].iteritems():
                for frame_id, index in indices.iteritems():

                    for item in index:
                        positions_index[text_field][term][frame_id].remove(item)
                        if not positions_index[text_field][term][frame_id]:
                            del positions_index[text_field][term][frame_id]

                    if not positions_index[text_field][term]:  # No items stored?
                        del positions_index[text_field][term]
                        delete_positions_keys.add(term)

        # Associations
        for text_field in text_fields:
            for term, value in new_associations[text_field].iteritems():
                for other_term, count in value.iteritems():
                    assocs_index[text_field][term][other_term] = assocs_index[
                        text_field][term].get(other_term, 0) + count
            for term, value in rm_associations[text_field].iteritems():
                for other_term, count in value.iteritems():
                    assocs_index[text_field][term][other_term] = assocs_index[text_field][term][other_term] - count
                    if assocs_index[text_field][term][other_term] == 0:
                        del assocs_index[text_field][term][other_term]
                if not assocs_index[text_field][term]:  # No associations recorded
                    del assocs_index[text_field][term]
                    delete_assoc_keys.add(term)

        # Frequencies
        for text_field in text_fields:
            for key, value in new_frequencies[text_field].iteritems():
                frequencies_index[text_field][key] = frequencies_index[text_field][key] + value
            for key, value in rm_frequencies[text_field].iteritems():
                frequencies_index[text_field][key] = frequencies_index[text_field][key] - value
                if not frequencies_index[text_field][key]:
                    del frequencies_index[text_field][key]
                    delete_frequencies_keys.add(key)

        # Metadata
        for name, values in new_metadata.iteritems():
            for value, f_ids in values.iteritems():
                try:
                    metadata_index[name][value].extend(f_ids)
                except KeyError:
                    metadata_index[name][value] = f_ids
        for name, values in rm_metadata.iteritems():
            for value, f_ids in values.iteritems():
                metadata_index[name][str(value)] = list(set(metadata_index[name][str(value)]) - set(f_ids))
                if not metadata_index[name][str(value)]:
                    del metadata_index[name][str(value)]
            if not metadata_index[name]:
                del metadata_index[name]
                delete_metadata_keys.add(name)

        # Now do the writing, starting with per field containers for indexed text
        for text_field in text_fields:
            # Positions
            for key in positions_index[text_field]:
                positions_index[text_field][key] = json.dumps(positions_index[text_field][key])
            self.__storage.set_container_items(
                IndexWriter.POSITIONS_CONTAINER.format(text_field), positions_index[text_field])
            self.__storage.delete_container_items(
                IndexWriter.POSITIONS_CONTAINER.format(text_field), delete_positions_keys)
            # Associations
            for key in assocs_index[text_field]:
                assocs_index[text_field][key] = json.dumps(assocs_index[text_field][key])
            self.__storage.set_container_items(
                IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field), assocs_index[text_field])
            self.__storage.delete_container_items(
                IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field), delete_assoc_keys)
            # Frequencies
            for key in frequencies_index[text_field]:
                frequencies_index[text_field][key] = json.dumps(frequencies_index[text_field][key])
            self.__storage.set_container_items(IndexWriter.FREQUENCIES_CONTAINER.format(
                text_field), frequencies_index[text_field])
            self.__storage.delete_container_items(
                IndexWriter.FREQUENCIES_CONTAINER.format(text_field), delete_frequencies_keys)

        # Use the actual fields for adding the frames - to handle the surrogate frames created when no
        # text field is present.
        for field in self.__new_frames.keys():
            self.__storage.set_container_items(
                IndexWriter.FRAMES_CONTAINER.format(field), {
                    k: json.dumps(v) for k, v in self.__new_frames[field].iteritems()
                }
            )

        for field in self.__rm_frames.keys():
            self.__storage.delete_container_items(
                IndexWriter.FRAMES_CONTAINER.format(field), self.__rm_frames[field]
            )

        self.__new_frames = {}
        self.__rm_frames = {}
        # Metadata
        for key in metadata_index:
            metadata_index[key] = json.dumps(metadata_index[key])
        self.__storage.set_container_items(IndexWriter.METADATA_CONTAINER, metadata_index)
        self.__storage.delete_container_items(IndexWriter.METADATA_CONTAINER, delete_metadata_keys)
        # Documents
        if self.__new_documents:
            self.__storage.set_container_items(IndexWriter.DOCUMENTS_CONTAINER,
                                               {k: json.dumps(v) for k, v in self.__new_documents.iteritems()})
        if self.__rm_documents:
            self.__storage.delete_container_items(IndexWriter.DOCUMENTS_CONTAINER, self.__rm_documents)
        self.__new_documents = {}
        self.__rm_documents = set()

    def commit(self):
        """Commit changes made by this writer by calling :meth:`.flush` then ``commit()`` on the storage instance."""
        self.flush()
        # Update index revision
        self.__storage.set_container_item(IndexWriter.INFO_CONTAINER, 'revision',
                                          json.dumps(random.SystemRandom().randint(0, 10**10)))
        self.__storage.commit()
        self.__committed = True

    def rollback(self):
        """Rollback any changes made by this writer."""
        self.__storage.rollback()
        self.__committed = True

    def close(self):
        """
        Close this writer.

        Calls :meth:`.rollback` if we are in the middle of a transaction.

        """
        # Do we need to rollback?
        if not self.__committed:
            logger.info('IndexWriter transaction wasn\'t committed, rolling back....')
            self.rollback()
        # Release the lock
        logger.debug("Releasing index write lock for {}....".format(self._path))
        self.__lock.release()
        # Close the storage connection
        self.__storage.close()
        self.__storage = None

    def add_document(self, frame_size=2, encoding='utf-8', encoding_errors='strict', **fields):
        """
        Add a document to this index.

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

        This method will call :meth:`.flush` if the internal buffers go over :const:`.RAM_BUFFER_SIZE`.

        Returns the id (str) of the document added.

        Internally what is happening here is that a document is broken up into its fields and a mini-index of the
        document is generated and stored with our buffers for writing out later.

        """
        logger.debug('Adding document')
        schema_fields = self.__schema.items()
        document_id = uuid.uuid4().hex
        sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

        # Build the frames by performing required analysis.
        frames = {}  # Frame data:: field_name -> {frame_id -> {key: value}}
        metadata = {}  # Inverted frame metadata:: field_name -> field_value
        frame_ids = {}  # List of frame_id's across all fields.

        # Shell frame includes all non-indexed and categorical fields
        shell_frame = {}
        for field_name, field in schema_fields:
            if (not field.indexed or field.categorical) and field.stored and field_name in fields:
                shell_frame[field_name] = fields[field_name]

        # Tokenize fields that need it
        logger.debug('Starting tokenization of document {}'.format(document_id))
        frame_count = 0
        for field_name, field in schema_fields:

            if field_name not in fields or not field.indexed or fields[field_name] is None:
                # Skip non-indexed fields or fields with no value supplied for this document
                continue

            if field.categorical:
                # Record categorical values
                for token in field.analyse(fields[field_name]):
                    try:
                        metadata[field_name].append(token.value)
                    except KeyError:
                        metadata[field_name] = [token.value]
            else:
                # Start the index for this field
                frames[field_name] = {}
                # Index non-categorical fields
                field_data = fields[field_name]
                expected_types = (str, bytes, unicode)
                if isinstance(field_data, str) or isinstance(field_data, bytes):
                    try:
                        field_data = fields[field_name] = field_data.decode(encoding, encoding_errors)
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
                        # Build our frames
                        frame_id = "{}-{}".format(document_id, frame_count)
                        try:
                            frame_ids[field_name].append(frame_id)
                        except KeyError:
                            frame_ids[field_name] = [frame_id]
                        frame_count += 1
                        frame = {
                            '_id': frame_id,
                            '_field': field_name,
                            '_positions': {},
                            '_sequence_number': frame_count,
                            '_doc_id': document_id,
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
                                        frame['_positions'][token.value].append(token.index)
                                    except KeyError:
                                        frame['_positions'][token.value] = [token.index]

                        # Build the final frame and add to the index
                        frame.update(shell_frame)
                        frames[field_name][frame_id] = frame

        # Currently only frames are searchable. That means if a schema contains no text fields it isn't searchable
        # at all. This block constructs a surrogate frame for storage in a catchall container to handle this case.
        if not frames and metadata:
            frame_id = "{}-{}".format(document_id, frame_count)
            frame = {
                '_id': frame_id,
                '_field': '',  # There is no text field
                '_positions': {},
                '_sequence_number': frame_count,
                '_doc_id': document_id,
            }
            frame.update(shell_frame)
            frames[''] = {frame_id: frame}
            frame_ids[''] = [frame_id]

        # Store the complete metadata in each item
        for field_name, values in frames.iteritems():
            for f_id in values:
                values[f_id]['_metadata'] = metadata

        logger.debug('Tokenization of document {} complete. {} frames created.'.format(document_id, len(frames)))

        # Store the frames for each field in our temporary buffer
        for key in frames.keys():
            try:
                self.__new_frames[key].update(frames[key])
            except KeyError:
                self.__new_frames[key] = frames[key]

        # Finally add the document to storage.
        doc_fields = {
            '_id': document_id,
            '_frames': frame_ids  # List of frames for this doc
        }

        for field_name, field in schema_fields:
            if field.stored and field_name in fields:
                # Only record stored fields against the document
                doc_fields[field_name] = fields[field_name]

        self.__new_documents[document_id] = doc_fields

        # Flush buffers if needed
        current_buffer_size = sys.getsizeof(self.__new_documents) + sys.getsizeof(self.__new_frames) + \
            sys.getsizeof(self.__rm_frames) + sys.getsizeof(self.__rm_documents)
        if current_buffer_size > IndexWriter.RAM_BUFFER_SIZE:
            logger.debug('Flushing index writer buffers')
            self.flush()

        return document_id

    def delete_document(self, d_id):
        """
        Delete the document with given ``d_id`` (str).

        Raises a :exc:`DocumentNotFound` exception if the d_id doesn't match any document.

        """
        try:
            doc = json.decode(self.__storage.get_container_item(IndexWriter.DOCUMENTS_CONTAINER, d_id))
        except KeyError:
            raise DocumentNotFoundError("No such document {}".format(d_id))

        for field, frames in doc['_frames'].iteritems():
            try:
                self.__rm_frames[field] += frames
            except KeyError:
                self.__rm_frames[field] = frames
        self.__rm_documents.add(d_id)

    def fold_term_case(self, text_field, merge_threshold=0.7):
        """
        Perform case folding on this index, merging words into names (camel cased word or phrase) and vice-versa
        depending ``merge_threshold``.

        ``merge_threshold`` (float) is used to test when to merge two variants. When the ratio between word and name
        version of a term falls below this threshold the merge is carried out.

        .. warning::
            This method calls flush before it runs and doesn't use the internal buffers.

        """
        self.flush()
        count = 0
        # Pre-fetch indexes and pass them around to save I/O
        frequencies_index = {
            k: json.loads(v) if v else 0
            for k, v in self.__storage.get_container_items(IndexWriter.FREQUENCIES_CONTAINER.format(text_field))
        }
        associations_index = {
            k: json.loads(v) if v else {}
            for k, v in self.__storage.get_container_items(IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field))
        }
        positions_index = {
            k: json.loads(v) if v else {}
            for k, v in self.__storage.get_container_items(IndexWriter.POSITIONS_CONTAINER.format(text_field))
        }
        frames = {
            k: json.loads(v)
            for k, v in self.__storage.get_container_items(IndexWriter.FRAMES_CONTAINER.format(text_field))
        }
        for w, freq in frequencies_index.items():
            if w.islower() and w.title() in frequencies_index:
                freq_name = frequencies_index[w.title()]
                if freq / freq_name < merge_threshold:
                    # Merge into name
                    logger.debug(u'Merging {} into {}'.format(w, w.title()))
                    self._merge_terms(w, w.title(), associations_index, positions_index, frequencies_index, frames)
                    count += 1
                elif freq_name / freq < merge_threshold:
                    # Merge into word
                    logger.debug(u'Merging {} into {}'.format(w.title(), w))
                    self._merge_terms(w.title(), w, associations_index, positions_index, frequencies_index, frames)
                    count += 1

        # Update stored data structures
        self.__storage.clear_container(IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field))
        self.__storage.set_container_items(IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field),
                                           {k: json.dumps(v) for k, v in associations_index.iteritems()})
        self.__storage.clear_container(IndexWriter.POSITIONS_CONTAINER.format(text_field))
        self.__storage.set_container_items(IndexWriter.POSITIONS_CONTAINER.format(text_field),
                                           {k: json.dumps(v) for k, v in positions_index.iteritems()})
        self.__storage.clear_container(IndexWriter.FREQUENCIES_CONTAINER.format(text_field))
        self.__storage.set_container_items(IndexWriter.FREQUENCIES_CONTAINER.format(text_field),
                                           {k: json.dumps(v) for k, v in frequencies_index.iteritems()})
        self.__storage.set_container_items(IndexWriter.FRAMES_CONTAINER.format(text_field),
                                           {f_id: json.dumps(d) for f_id, d in frames.iteritems()})
        logger.debug("Merged {} terms during case folding.".format(count))

    def merge_terms(self, merges, text_field):
        """
        Merge the terms in ``merges`` for the given ``text_field``. ``text_field`` must be of type TEXT.

        ``merges`` (list) should be a list of str tuples of the format ``(old_term, new_term,)``. If new_term is ``''``
        then old_term is removed. N-grams can be specified by supplying a str tuple instead of str for the old term.
        For example::

            >>> (('hot', 'dog'), 'hot dog')

        The n-gram case does not allow empty values for new_term.

        .. warning::
            This method calls flush before it runs and doesn't use the internal buffers.

        """
        self.flush()
        count = 0
        # Pre-fetch indexes and pass them around to save I/O

        frequencies_index = {
            k: json.loads(v) if v else 0
            for k, v in self.__storage.get_container_items(IndexWriter.FREQUENCIES_CONTAINER.format(text_field))
        }

        associations_index = {
            k: json.loads(v) if v else {}
            for k, v in self.__storage.get_container_items(IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field))
        }

        positions_index = {
            k: json.loads(v) if v else {}
            for k, v in self.__storage.get_container_items(IndexWriter.POSITIONS_CONTAINER.format(text_field))
        }

        frames = {
            k: json.loads(v)
            for k, v in self.__storage.get_container_items(IndexWriter.FRAMES_CONTAINER.format(text_field))
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
        self.__storage.clear_container(IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field))
        self.__storage.set_container_items(IndexWriter.ASSOCIATIONS_CONTAINER.format(text_field),
                                           {k: json.dumps(v) for k, v in associations_index.iteritems()})
        self.__storage.clear_container(IndexWriter.POSITIONS_CONTAINER.format(text_field))
        self.__storage.set_container_items(IndexWriter.POSITIONS_CONTAINER.format(text_field),
                                           {k: json.dumps(v) for k, v in positions_index.iteritems()})
        self.__storage.clear_container(IndexWriter.FREQUENCIES_CONTAINER.format(text_field))
        self.__storage.set_container_items(IndexWriter.FREQUENCIES_CONTAINER.format(text_field),
                                           {k: json.dumps(v) for k, v in frequencies_index.iteritems()})
        self.__storage.set_container_items(IndexWriter.FRAMES_CONTAINER.format(text_field),
                                           {f_id: json.dumps(d) for f_id, d in frames.iteritems()})
        logger.debug("Merged {} terms during manual merge.".format(count))

    def _merge_terms(self, old_term, new_term, associations, positions, frequencies, frames):
        """
        Merge ``old_term`` into ``new_term``, operating in-place on the provided index components.

        If ``new_term`` is a false-y value, ``old_term`` is deleted. Updates all the passed index structures to reflect
        the change.

        Raises :exc:`TermNotFoundError` if the term does not exist in the index.

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

    def _merge_terms_into_ngram(self, ngram_terms, new_term, associations, positions, frequencies, frames, max_gap=2):
        """
        Merge n-gram term sequences into ``new_term`` and update all the passed index structures to reflect the change.

        This method cannot be used to delete terms, so a falsey value for ``new_term`` will produce an exception. Also,
        raises :exc:`TermNotFoundError` if the n-gram does not exist in the index.

        ``max_gap`` (int) specifies the maximum number of characters allowed between n-gram terms.

        """
        if not new_term:
            raise ValueError('A non-empty value must be specified to represent the n-gram')

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
                        if curr_pos[0] < next_pos[0] and next_pos[0] - curr_pos[1] <= max_gap:
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

    def set_plugin_state(self, plugin):
        """ Write the state of the given plugin to the index.

        Any existing state for this plugin instance will be overwritten.

        """
        # low level calls to plugin storage subsystem.
        self.__storage.set_plugin_state(plugin.get_name(),
                                        plugin.get_settings(),
                                        plugin.get_state())

    def delete_plugin_instance(self, plugin):
        """
        Delete the state corresponding to the given plugin instance.

        """
        self.__storage.delete_plugin_state(plugin.get_name(), plugin_settings=plugin.get_settings())

    def delete_plugin(self, plugin_name):
        """
        Delete all plugin data corresponding to the given plugin name.

        """
        self.__storage.delete_plugin_state(plugin_name)

    def add_fields(self, **fields):
        """
        Add new fields to the schema.

        All keyword arguments are treated as ``(field_name, field_type)`` pairs.

        """
        for field_name, value in fields.iteritems():
            self.__schema.add(field_name, value)
            if field_name in self.__schema.get_indexed_text_fields():
                self._init_text_field(field_name)
        self.__config.schema = self.__schema
        # Save updated schema
        with open(os.path.join(self._path, IndexWriter.CONFIG_FILE), 'w') as f:
            f.write(self.__config.dumps())

    def set_setting(self, name, value):
        """Set the ``value`` of setting identified by ``name``."""
        self.__storage.set_container_item(IndexWriter.SETTINGS_CONTAINER, name, json.dumps(value))

    def _init_text_field(self, field):
        """Initialise storage containers for text ``field``."""
        storage = self.__config.storage_cls(self._path)
        storage.begin()
        storage.add_container(IndexWriter.FRAMES_CONTAINER.format(field))
        storage.add_container(IndexWriter.POSITIONS_CONTAINER.format(field))
        storage.add_container(IndexWriter.ASSOCIATIONS_CONTAINER.format(field))
        storage.add_container(IndexWriter.FREQUENCIES_CONTAINER.format(field))
        storage.commit()


class IndexReader(object):

    """
    Read information from an existing index.

    Once an IndexReader is opened, it will **not** see any changes written to the index by an :class:`IndexWriter`. To
    see any new changes you must open a new IndexReader.

    To search an index, use :meth:`.searcher` to fetch an :class:`caterpillar.searching.IndexSearcher` instance to
    execute the search with. A searcher will only work while this IndexReader remains open.

    Access to the raw underlying associations, frequencies and positions index is provided by this class but a caller
    needs to be aware that these may consume a **LARGE** amount of memory depending on the size of the index. As such,
    all access to these indexes are provided by generators (see :meth:`.get_frames` for example).

    Once you are finished with an IndexReader you need to call the :meth:`.close` method.

    IndexReader is a context manager and can be used via the with statement to make this easier. For example::

        >>> with IndexReader('/path/to/index') as r:
        ...    # Do stuff
        ...    doc = r.get_document(d_id)
        >>> # Reader is closed

    .. warning::
        While opening an IndexReader is quite cheap, it definitely isn't free. If you are going to do a large amount of
        reading over a relatively short time span, it is much better to do so using one reader.

    There is no limit to the number of IndexReader objects which can be active on an index. IndexReader objects are also
    thread-safe.

    IndexReader doesn't cache any data. Every time you ask for data, the underlying :class:`caterpillar.storage.Storage`
    instance is used to fetch that data. If you were to call :meth:`,get_associations_index` 10 times, each time the
    data will be fetched from the storage instance and not some internal cache. The underlying storage instance may do
    some of it's own caching but that is transparent to us.

    """

    def __init__(self, path):
        """
        Open a new IndexReader for the index at ``path`` (str).

        This constructor only creates the instance. Before you start reading you need to call :meth:`.begin` which is
        automatically called via :meth:`.__enter__`.

        """
        self.__path = path
        try:
            with open(os.path.join(path, IndexWriter.CONFIG_FILE), "r") as f:
                self.__config = IndexConfig.loads(f.read())
            self.__storage = self.__config.storage_cls(path, readonly=True)
        except StorageNotFoundError:
            logger.exception("Couldn't open storage for {}".format(path))
            raise IndexNotFoundError("Couldn't find an index at {} (no storage)".format(path))
        except IOError:
            logger.exception("Couldn't read index config for {}".format(path))
            raise IndexNotFoundError("Couldn't find an index at {} (no config)".format(path))
        else:
            self.__schema = self.__config.schema

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def begin(self):
        """
        Begin reading with this IndexReader.

        From this point on, no changes to the underlying index made by an :class:`IndexWriter` will be seen.

        .. warning::
            This method **must** be called before any reading can be done.

        """
        self.__storage.begin()

    def close(self):
        """
        Release all resources used by this IndexReader.

        Calling this method renders this instance unusable.

        """
        self.__storage.commit()
        self.__storage.close()

    def get_positions_index(self, field):
        """
        Get all term positions for the given indexed text field.

        This is a generator which yields a key/value pair tuple.

        This is what is known as an inverted text index. Structure is as follows::

            {
                "term": {
                    "frame_id": [(start, end), (start, end)],
                    ...
                },
                ...
            }

        """
        for k, v in self.__storage.get_container_items(IndexWriter.POSITIONS_CONTAINER.format(field)):
            yield (k, json.loads(v))

    def get_term_positions(self, term, field):
        """
        Returns a dict of term positions for ``term`` (str).

        Structure of returned dict is as follows::

        {
            frame_id1: [(start, end), (start, end)],
            frame_id2: [(start, end), (start, end)],
            ...
        }

        """
        return json.loads(self.__storage.get_container_item(IndexWriter.POSITIONS_CONTAINER.format(field), term))

    def get_associations_index(self, field):
        """
        Term associations for this Index.

        This is used to record when two terms co-occur in a document. Be aware that only 1 co-occurrence for two terms
        is recorded per document no matter the frequency of each term. The format is as follows::

            {
                term: {
                    other_term: count,
                    ...
                },
                ...
            }

        This method is a generator which yields key/value pair tuples.

        """
        for k, v in self.__storage.get_container_items(IndexWriter.ASSOCIATIONS_CONTAINER.format(field)):
            yield (k, json.loads(v))

    def get_term_association(self, term, association, field):
        """Returns a count of term associations between ``term`` (str) and ``association`` (str)."""
        return json.loads(self.__storage.get_container_item(IndexWriter.ASSOCIATIONS_CONTAINER.format(field),
                                                            term))[association]

    def get_frequencies(self, field):
        """
        Term frequencies for this index.

        Be aware that a terms frequency is only incremented by 1 per frame no matter the frequency within that
        frame. The format is as follows::

            {
                term: count
            }

        This method is a generator that yields key/value paris of tuples (term, count).

        .. note::
            If you want to get the term frequency at a document level rather then a frame level then you should count
            all of the terms positions returned by :meth:`.get_term_position`.

        """
        for k, v in self.__storage.get_container_items(IndexWriter.FREQUENCIES_CONTAINER.format(field)):
            yield (k, json.loads(v))

    def get_term_frequency(self, term, field):
        """Return the frequency of ``term`` (str) as an int."""
        return json.loads(self.__storage.get_container_item(IndexWriter.FREQUENCIES_CONTAINER.format(field), term))

    def get_frame_count(self, field):
        """Return the int count of frames stored on this index."""
        return self.__storage.get_container_len(IndexWriter.FRAMES_CONTAINER.format(field))

    def get_frame(self, frame_id, field):
        """Fetch frame ``frame_id`` (str)."""
        return json.loads(self.__storage.get_container_item(IndexWriter.FRAMES_CONTAINER.format(field), frame_id))

    def get_frames(self, field, frame_ids=None,):
        """
        Generator across frames from this field in this index.

        If present, the returned frames will be restricted to those with ids in ``frame_ids`` (list). Format of the
        frames index data is as follows::

            {
                frame_id: { //framed data },
                frame_id: { //framed data },
                frame_id: { //framed data },
                ...
            }

        This method is a generator that yields tuples of frame_id and frame data dict.

        """
        for k, v in self.__storage.get_container_items(IndexWriter.FRAMES_CONTAINER.format(field), keys=frame_ids):
            yield (k, json.loads(v))

    def get_frame_ids(self, field):
        """Generator of ids for all frames stored on this index."""
        for f_id in self.__storage.get_container_keys(IndexWriter.FRAMES_CONTAINER.format(field)):
            yield f_id

    def get_document(self, document_id):
        """Returns the document with the given ``document_id`` (str) as a dict."""
        try:
            return json.loads(self.__storage.get_container_item(IndexWriter.DOCUMENTS_CONTAINER, document_id))
        except KeyError:
            raise DocumentNotFoundError("No document '{}'".format(document_id))

    def get_document_count(self):
        """Returns the int count of documents added to this index."""
        return self.__storage.get_container_len(IndexWriter.DOCUMENTS_CONTAINER)

    def get_documents(self, document_ids=None):
        """
        Generator that yields documents from this index as (id, data) tuples.

        If present, the returned documents will be restricted to those with ids in ``document_ids`` (list).

        """
        for k, v in self.__storage.get_container_items(IndexWriter.DOCUMENTS_CONTAINER, keys=document_ids):
            yield (k, json.loads(v))

    def get_metadata(self):
        """
        Get the metadata index.

        This method is a generator that yields a key, value tuple. The index is in the following format::

            {
                "field_name": {
                    "value": ["frame_id", "frame_id"],
                    "value": ["frame_id", "frame_id"],
                    "value": ["frame_id", "frame_id"],
                    ...
                },
                ...
            }

        """
        for k, v in self.__storage.get_container_items(IndexWriter.METADATA_CONTAINER):
            yield (k, json.loads(v))

    def get_schema(self):
        """Get the :class:`caterpillar.processing.schema.Schema` for this index."""
        return self.__schema

    def get_revision(self):
        """
        Return the str revision identifier for this index.

        The revision identifier is a version identifier. It gets updated every time the index gets changed.

        """
        return self.__storage.get_container_item(IndexWriter.INFO_CONTAINER, 'revision')

    def get_vocab_size(self, field):
        """Get total number of unique terms identified for this index (int)."""
        return self.__storage.get_container_len(IndexWriter.POSITIONS_CONTAINER.format(field))

    def searcher(self, scorer_cls=TfidfScorer):
        """
        Return an :class:`IndexSearcher <caterpillar.search.IndexSearcher>` for this Index.

        """
        return IndexSearcher(self, scorer_cls)

    def get_setting(self, name):
        """Get the setting identified by ``name`` (str)."""
        try:
            return json.loads(self.__storage.get_container_item(IndexWriter.SETTINGS_CONTAINER, name))
        except KeyError:
            raise SettingNotFoundError("No setting '{}'".format(name))

    def get_settings(self, names):
        """
        All settings listed in ``names`` (list).

        This method is a generator that yields name/value pair tuples. The format of the settings index is::

            {
                name: value,
                name: value,
                ...
            }

        """
        for k, v in self.__storage.get_container_items(IndexWriter.SETTINGS_CONTAINER, keys=names):
            if v:
                yield (k, json.loads(v))

    def get_plugin_state(self, plugin):
        """
        Returns the state of the given plugin stored in the index.

        """
        return dict(self.__storage.get_plugin_state(plugin.get_name(), plugin.get_settings()))

    def get_plugin_by_id(self, plugin_id):
        """
        Returns the settings and state corresponding to the given plugin_id.

        """
        settings, state = self.__storage.get_plugin_by_id(plugin_id)
        return settings, dict(state)

    def list_plugins(self):
        """
        List all plugin instances that have been stored in this index.
        """
        return self.__storage.list_known_plugins()


def find_bi_gram_words(frames, min_count=5, threshold=40.0):
    """
    This function finds bi-gram words from the specified ``frames`` iterable.

    For two terms to be considered a bi-gram it must first occur at least ``min_count`` (int) times across all frames.
    Subsequently, a score is calculated and bi-grams are included based on the specified ``threshold`` (float).

    The formula for calculating bi-gram score is inspired by the Gensim implementation of phrase detection from the
    Mikolov et al paper, "Distributed Representations of Words and Phrases and their Compositionality".

        score(a, b) = freq(a, b) * vocab_size / (freq(a) * freq(b))

    The multiplication of frequencies in the denominator diminishes the bi-gram score when both uni-gram frequencies are
    significantly higher than the  bi-gram frequency. However, if only one uni-gram frequency is significantly higher
    bi-grams can still score sufficiently high to be included.

    The multiplication by vocab_size in the numerator boosts the score for larger corpora where uni-grams may be found
    in many contexts.

    The ``threshold`` value is derived empirically with the aim of capturing the most discriminative bi-grams.

    This function uses a :class:`caterpillar.processing.analysis.analyse.PotentialBiGramAnalyser` to identify potential
    bi-grams. Names and stopwords are not considered for bi-grams.

    Returns a list of bi-gram strings that pass the criteria.

    """
    logger.debug("Identifying n-grams")

    # Generate a table of candidate bigrams
    candidate_bi_grams = nltk.probability.FreqDist()
    uni_gram_frequencies = nltk.probability.FreqDist()
    bi_gram_analyser = PotentialBiGramAnalyser()
    sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    num_frames = 0
    for _, frame in frames:
        for sentence in sentence_tokenizer.tokenize(frame['_text'], realign_boundaries=True):
            terms_seen = []
            for token_list in bi_gram_analyser.analyse(sentence):
                # Using a special filter that returns list of tokens. List of 1 means no bi-grams.
                if len(token_list) > 1:  # We have a bi-gram people!
                    candidate_bi_grams.inc(u"{} {}".format(token_list[0].value, token_list[1].value))
                for t in token_list:  # Keep a list of terms we have seen so we can record freqs later.
                    if not t.stopped:  # Naughty stopwords!
                        terms_seen.append(t.value)
            for term in terms_seen:
                uni_gram_frequencies.inc(term)
        num_frames += 1

    # Filter and sort by frequency-decreasing
    def filter_bi_grams(b):
        k, v = b
        if v < min_count:
            return False
        t1, t2 = k.split(" ")
        score = v / (uni_gram_frequencies[t1] * uni_gram_frequencies[t2]) * len(uni_gram_frequencies)
        return score > threshold
    candidate_bi_gram_list = filter(filter_bi_grams, candidate_bi_grams.iteritems())
    logger.debug("Identified {} n-grams.".format(len(candidate_bi_gram_list)))
    return [b[0] for b in candidate_bi_gram_list]
