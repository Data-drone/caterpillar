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
    >>> with index.IndexReader('/tmp/test_index') as reader:
    ...     reader.get_document_count()
    ...
    1

"""

from __future__ import absolute_import, division, unicode_literals

import logging
import os
import cPickle
import ujson as json

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
        self._storage_reader_cls = storage_cls.reader
        self._storage_writer_cls = storage_cls.writer
        self._schema = schema
        self._version = VERSION

    @property
    def storage_reader_cls(self):
        return self._storage_reader_cls

    @property
    def storage_writer_cls(self):
        return self._storage_writer_cls

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

    Once you have performed all the writes/deletes you like you need to call :meth:`.commit` to finalise the
    transaction. Alternatively, if there was a problem during your transaction, you can call :meth:`.rollback` instead
    to revert any changes you made using this writer. **IMPORTANT** - Finally, you need to call :meth:`.close` to
    release the lock.

    Using IndexWriter this way should look something like this:

        >>> writer = IndexWriter('/some/path/to/an/index')
        >>> try:
        ...     writer.begin(timeout=2)  # timeout in 2 seconds
        ...     # Do stuff, like add_document() etc...
        ...     writer.commit()  # Write the changes...
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

    # Where is the config?
    CONFIG_FILE = "index.config"

    def __init__(self, path, config=None):
        """
        Open an existing index for writing or create a new index for writing.

        If ``path`` (str) doesn't exist and ``config`` is not None, then a new index will created when :meth:`begin` is
        called (after the lock is acquired). Otherwise, :exc:`IndexNotFoundError` is raised.

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
            self.__storage = self.__config.storage_writer_cls(path, create=False)
            self.__schema = self.__config.schema
            self.__lock = None  # Should declare in __init__ and not outside.
        self.__committed = False

        # Attribute to store the details of the most recent commit
        self.last_committed_documents = []
        self.last_deleted_documents = []
        self.last_updated_plugins = []

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
                storage = self.__config.storage_writer_cls(self._path, create=True)

                # Initialise our fields:
                storage.begin()

                storage.add_unstructured_fields([''])  # Metadata hack until we have document search
                storage.add_unstructured_fields(self.__schema.get_indexed_text_fields())
                storage.add_structured_fields(self.__schema.get_indexed_structured_fields())

                storage.commit()

            if not self.__storage:
                # This is a create or the index was created after this writer was opened.
                self.__storage = self.__config.storage_writer_cls(self._path, create=False)

            self.__storage.begin()

    def commit(self):
        """Commit changes made by this writer by calling :meth:``commit()`` on the storage instance."""
        self.last_committed_documents, self.last_deleted_documents, self.last_updated_plugins = self.__storage.commit()
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

        # Close the storage connection
        self.__storage.close()
        self.__storage = None

        # Release the lock
        logger.debug("Releasing index write lock for {}....".format(self._path))
        self.__lock.release()

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

        Documents are assigned an ID only at commit time. the attribute ``last_committed_documents`` of the
        writer contains the ID's of the documents added in the last completed write transaction for that
        writer.

        """
        logger.debug('Adding document')
        schema_fields = self.__schema.items()
        sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

        # Build the frames by performing required analysis.
        frames = {}  # Frame data:: field_name -> [frame1, frame2, frame3]
        term_positions = {}  # Term vector data:: field_name --> [{term1:freq, term2:freq}, {term2:freq, term3:freq}]

        metadata = {}  # Inverted frame metadata:: field_name -> field_value

        # Shell frame includes all non-indexed and categorical fields
        shell_frame = {}
        for field_name, field in schema_fields:
            if (not field.indexed or field.categorical) and field.stored and field_name in fields:
                shell_frame[field_name] = fields[field_name]

        # Tokenize fields that need it
        logger.debug('Starting tokenization of document')
        frame_count = 0

        # Analyze document level structured fields separately to inject in the frames.
        for field_name, field in schema_fields:

            if field_name not in fields or fields[field_name] is None \
                    or not field.indexed or not field.categorical:
                # Skip fields not supplied or with empty values for this document.
                continue

            # Record categorical values
            for token in field.analyse(fields[field_name]):
                metadata[field_name] = token.value

        # Now just the unstructured fields
        for field_name, field in schema_fields:

            if field_name not in fields or fields[field_name] is None \
                    or not field.indexed or field.categorical:
                continue

            # Start the index for this field
            frames[field_name] = []
            term_positions[field_name] = []

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
                                    frame['_positions'][token.value].append(token.index)
                                except KeyError:
                                    frame['_positions'][token.value] = [token.index]

                    # Build the final frame and add to the index
                    frame.update(shell_frame)
                    # Serialised representation of the frame
                    frames[field_name].append(json.dumps(frame))

                    # Generate the term-frequency vector for the frame:
                    term_positions[field_name].append(frame['_positions'])

        # Currently only frames are searchable. That means if a schema contains no text fields it isn't searchable
        # at all. This block constructs a surrogate frame for storage in a catchall container to handle this case.
        if not frames and metadata:
            frame = {
                '_field': '',  # There is no text field
                '_positions': {},
                '_sequence_number': frame_count,
                '_metadata': metadata
            }
            frame.update(shell_frame)
            try:
                frames[''].append(json.dumps(frame))
            except KeyError:
                frames[''] = [json.dumps(frame)]
            try:
                term_positions[''].append(frame['_positions'])
            except:
                term_positions[''] = [frame['_positions']]

        # Finally add the document to storage.
        doc_fields = {}

        for field_name, field in schema_fields:
            if field.stored and field_name in fields:
                # Only record stored fields against the document
                doc_fields[field_name] = fields[field_name]

        document = json.dumps(doc_fields)

        self.__storage.add_analyzed_document('v1', (document, metadata, frames, term_positions))

        logger.debug('Tokenization of document complete. {} frames staged for storage.'.format(len(frames)))

    def delete_document(self, document_id):
        """
        Delete the document with given ``document_id`` (str).

        If the document does not exist, no error will be raised. The ``IndexWriter`` attribute
        ``last_deleted_documents`` contains the ID's of documents that were present in the
        index and deleted during the last transaction.

        """
        self.__storage.delete_documents([document_id])

    def fold_term_case(self, text_field, merge_threshold=0.7):
        """
        Perform case folding on this index, merging words into names (camel cased word or phrase) and vice-versa
        depending ``merge_threshold``.

        ``merge_threshold`` (float) is used to test when to merge two variants. When the ratio between word and name
        version of a term falls below this threshold the merge is carried out.

        The statistics used to determine which terms to fold are calculated from ``text_field`` and only instances
        of terms in that field will be modified.

        This method only works on content committed to disk. It will need to be run in a separate writer if you
        wish to fold newly added documents.

        """

        merges = []

        reader = IndexReader(self._path)
        frequencies_index = dict(reader.get_frequencies(text_field))
        for w, freq in frequencies_index.iteritems():
            if w.islower() and w.title() in frequencies_index:
                freq_name = frequencies_index[w.title()]
                if freq / freq_name < merge_threshold:
                    # Merge into name
                    logger.debug(u'Merging {} into {}'.format(w, w.title()))
                    merges.append((w, w.title()))

                elif freq_name / freq < merge_threshold:
                    # Merge into word
                    logger.debug(u'Merging {} into {}'.format(w.title(), w))
                    merges.append((w.title(), w))

        count = len(merges)
        self.merge_terms(merges, text_field)

        logger.debug("Merged {} terms during case folding.".format(count))

    def merge_terms(self, merges, text_field, bigram_max_char_gap=2):
        """
        Merge the terms in ``merges`` across the whole index.

        ``merges`` (list) should be a list of str tuples of the format ``(old_term, new_term,)``. If new_term is ``''``
        then old_term is removed. N-grams can be specified by supplying a str tuple instead of str for the old term.
        For example::

            >>> (('hot', 'dog'), 'hot dog')

        """
        count = len(merges)

        # Run through merges, and dispatch to unigram/bigram merging as appropriate
        for terms, new_term in merges:
            if isinstance(terms, basestring):
                if new_term:
                    self.__storage._merge_term_variation(terms, new_term, text_field)
                else:  # Map falsey values to the empty string, removing them from consideration
                    self.__storage._merge_term_variation(terms, '', text_field)
            else:
                left_term, right_term = terms
                self.__storage._merge_bigrams(
                    terms[0], terms[1], new_term, text_field, max_char_gap=bigram_max_char_gap
                )

        logger.debug("Merged {} terms during manual merge.".format(count))

    def set_plugin_state(self, plugin):
        """ Write the state of the given plugin to the index.

        Any existing state for this plugin instance will be overwritten.

        The ID's of updated plugins are available in the last_updated_plugins attribute of the
        IndexWriter after the transaction is committed.

        """
        # low level calls to plugin storage subsystem.
        self.__storage.set_plugin_state(
            plugin.get_type(), plugin.get_settings(), plugin.get_state()
        )

    def delete_plugin_instance(self, plugin):
        """
        Delete the state corresponding to the given plugin instance.

        """
        self.__storage.delete_plugin_state(plugin.get_type(), plugin_settings=plugin.get_settings())

    def delete_plugin_type(self, plugin_type):
        """
        Delete all plugins and corresponding data of the specified ``plugin_type``.

        """
        self.__storage.delete_plugin_state(plugin_type)

    def add_fields(self, **fields):
        """
        Add new fields to the schema.

        All keyword arguments are treated as ``(field_name, field_type)`` pairs.

        """
        for field_name, field in fields.iteritems():
            self.__schema.add(field_name, field)
            if field_name in self.__schema.get_indexed_text_fields():
                self.__storage.add_unstructured_fields([field_name])
            if field_name in self.__schema.get_indexed_structured_fields():
                self.__storage.add_structured_fields([field_name])

        self.__config.schema = self.__schema
        # Save updated schema
        with open(os.path.join(self._path, IndexWriter.CONFIG_FILE), 'w') as f:
            f.write(self.__config.dumps())

    def set_setting(self, name, value):
        """Set the setting identified by ``name`` to ``value``."""
        self.__storage.set_setting(name, value)


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
            self.__storage = self.__config.storage_reader_cls(path)
        except StorageNotFoundError:
            logger.exception("Couldn't open storage for {}".format(path))
            raise IndexNotFoundError("Couldn't find an index at {} (no storage)".format(path))
        except IOError:
            logger.exception("Couldn't read index config for {}".format(path))
            raise IndexNotFoundError("Couldn't find an index at {} (no config)".format(path))
        else:
            self.__schema = self.__config.schema

    @property
    def revision(self):
        return self.__storage.revision

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
        return self.__storage._iterate_positions(include_fields=[field])

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
        try:
            positions = next(self.__storage._iterate_positions(terms=[term], include_fields=[field]))
            return positions[1]
        except StopIteration:
            raise KeyError('"{}" not found in field "{}"'.format(term, field))

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
        return self.__storage.iterate_associations(include_fields=[field])

    def get_term_association(self, term, association, field):
        """Returns a count of term associations between ``term`` (str) and ``association`` (str)."""

        term, associations = next(
            self.__storage.iterate_associations(term=term, association=association, include_fields=[field])
        )
        if term is None:
            raise KeyError('"{}" not associated with term "{}" or has no associations.'.format(association, term))

        count = associations[association]

        return count

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
        return self.__storage.iterate_term_frequencies(include_fields=[field])

    def get_term_frequency(self, term, field):
        """Return the frequency of ``term`` (str) as an int."""
        try:
            frequency = next(self.__storage.iterate_term_frequencies(terms=[term], include_fields=[field]))
            return frequency[1]
        except StopIteration:
            raise KeyError('"{}" not found in field "{}"'.format(term, field))

    def get_frame_count(self, field):
        """Return the int count of frames stored on this index."""
        return self.__storage.count_frames(include_fields=[field])

    def get_frame(self, frame_id, field):
        """Fetch frame ``frame_id`` (str)."""
        try:
            row = next(self.__storage.iterate_frames(frame_ids=[frame_id], include_fields=[field]))
            frame = json.loads(row[4])
            frame['_id'] = row[0]
            frame['_doc_id'] = row[1]
            return frame

        except StopIteration:
            raise DocumentNotFoundError

    def get_frames(self, field, frame_ids=None):
        """
        Generator across frames from this field in this index.

        If present, the returned frames will be restricted to those with ids in ``frame_ids`` (list). The field
        argument will be ignored if frame_ids are provided.
        Format of theframes index data is as follows::

            {
                frame_id: { //framed data },
                frame_id: { //framed data },
                frame_id: { //framed data },
                ...
            }

        This method is a generator that yields tuples of frame_id and frame data dict.

        """
        for row in self.__storage.iterate_frames(frame_ids=frame_ids, include_fields=[field]):
            frame = json.loads(row[4])
            frame['_id'] = row[0]
            frame['_doc_id'] = row[1]
            yield row[0], frame

    def get_frame_ids(self, field):
        """Generator of ids for all frames stored on this index."""
        for row in self.__storage.iterate_frames(include_fields=[field]):
            yield row[0]

    def get_document(self, document_id):
        """Returns the document with the given ``document_id`` (str) as a dict."""
        try:
            document = next(self.__storage.iterate_documents([document_id]))
            # inject _id field
            doc = json.loads(document[1])
            doc['_id'] = document[0]
            return doc

        except StopIteration:
            raise DocumentNotFoundError("No document '{}'".format(document_id))

    def get_document_count(self):
        """Returns the int count of documents added to this index."""
        return self.__storage.count_documents()

    def get_documents(self, document_ids=None):
        """
        Generator that yields documents from this index as (id, data) tuples.

        If present, the returned documents will be restricted to those with ids in ``document_ids`` (list).

        """
        return (
            (doc_id, json.loads(document))
            for doc_id, document in self.__storage.iterate_documents(document_ids=document_ids)
        )

    def get_metadata(self, text_field=None):
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

        The optional text_field limits the returned values to frames from that field.

        """
        metadata = self.__storage.iterate_metadata(text_field=text_field)

        current_field, value, frame_ids = next(metadata)
        current_values = {value: frame_ids}
        while True:
            try:
                field, value, frame_ids = next(metadata)
            except StopIteration:
                yield current_field, current_values
                break
            if field == current_field:
                current_values[value] = frame_ids
            else:
                yield current_field, current_values
                current_field = field
                current_values = {value: frame_ids}

    def get_schema(self):
        """Get the :class:`caterpillar.processing.schema.Schema` for this index."""
        return self.__schema

    def get_revision(self):
        """
        Return the str revision identifier for this index.

        The revision identifier is a version identifier. It gets updated every time the index gets changed.

        """
        return self.__storage.revision

    def get_vocab_size(self, field):
        """
        Get total number of unique terms identified for the specified field in this index (int).

        Note that terms may be shared across fields, so the sum of the vocab_size in each field will
        overcount the number of terms.

        """
        return self.__storage.count_vocabulary(include_fields=[field])

    def searcher(self, scorer_cls=TfidfScorer):
        """
        Return an :class:`IndexSearcher <caterpillar.search.IndexSearcher>` for this Index.

        """
        return IndexSearcher(self, scorer_cls)

    def get_setting(self, name):
        """Get the setting identified by ``name`` (str)."""
        setting_dict = self.__storage.get_settings([name]).fetchone()
        if setting_dict is None:
            raise SettingNotFoundError("No setting '{}'".format(name))
        else:
            return setting_dict[1]

    def get_settings(self, names):
        """
        A generator of all settings listed in ``names`` (list).

        Names that are not stored in the index are not included in the returned dictionary.

            {
                name: value,
                name: value,
                ...
            }

        """
        return self.__storage.get_settings(names)

    def get_plugin_state(self, plugin):
        """
        Returns the state of the given plugin stored in the index.

        """
        return dict(self.__storage.get_plugin_state(plugin.get_type(), plugin.get_settings()))

    def get_plugin_by_id(self, plugin_id):
        """
        Returns the plugin_type, settings and state corresponding to the given plugin_id.

        """
        plugin_type, settings, state = self.__storage.get_plugin_by_id(plugin_id)
        return plugin_type, settings, dict(state)

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

    Returns a list of bigram strings that pass the criteria.

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
                    bigram = u"{} {}".format(token_list[0].value, token_list[1].value)
                    candidate_bi_grams.inc(bigram)

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
