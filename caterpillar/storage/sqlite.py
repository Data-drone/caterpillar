# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""
An sqlite implementation of :class:`caterpillar.storage.Storage`.

The only class is :class:`.SqliteStorage` which uses sqlite in WAL mode to achieve reader/writer isolation.

All changes to an index are first staged to a temporary in-memory database, the main storage file is not
updated until commit is called. At this point all of the contents of the index are flushed to the file. By
leaving the final flush operation as a single SQL script we can drop the GIL and allow concurrent operation
in multiple threads.

Note that document deletes are 'soft' deletes. Wherever possible the document data is deleted, however in
the document_data and term_posting tables a hard delete requires a full table scan, so this is not ordinarily
performed.

"""
import logging
import os

import apsw

from caterpillar.storage import StorageWriter, StorageReader, Storage, StorageNotFoundError, \
    DuplicateStorageError, PluginNotFoundError
from ._schema import disk_schema, cache_schema, prepare_flush, flush_cache


logger = logging.getLogger(__name__)


class SqliteWriter(StorageWriter):
    """
    This class utilises SQLite to store data structures to disk.

    Reader / writer isolation here is provided by using `WAL mode <http://www.sqlite.org/wal.html>`_. There are no
    changes to the default checkpoint behaviour of SQLite, which at the time of writing defaults to 1000 pages.

    After initialisation all changes to the database are staged to a temporary in memory database. The changes are
    not flushed to persistent storage until the commit method of this storage object is called.

    """

    def __init__(self, path, create=False):
        """
        Initialise a new instance of this storage at ``path`` (str).

        If ``create`` (bool) is False and path doesn't exist then a :exc:`StorageNotFoundError` is raised. Otherwise,
        if ``create`` is True then we create the database if it doesn't already exist.

        If ``create`` is True and the DB already exist, then :exc:`DuplicateStorageError` is raised.

        """
        self._db_path = path
        self._db = os.path.join(path, 'storage.db')

        if not create and not os.path.exists(self._db):
            raise StorageNotFoundError('Can\'t find the resources required by SQLiteStorage. Is it corrupt?')
        elif create and os.path.exists(self._db):
            raise DuplicateStorageError('There already appears to be something stored at {}'.format(path))

        if create:
            connection = apsw.Connection(self._db, flags=apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE)
            cursor = connection.cursor()

            # Setup schema and necessary pragmas (on disk)
            # The database is never written to directly, hence it is closed after initilisation.
            list(cursor.execute(disk_schema))
            connection.close()

    def begin(self, writer=False):
        """
        Begin a transaction.

        The write flag indicates if this is a transaction for a reader or a writer.

        For a writer a temporary in-memory database is created to cache results, which
        is destroyed after the commit or rollback methods are called.

        """

        # If we're opening for writing, don't connect to the index directly.
        # Instead setup a temporary in memory database with the minimal schema.
        self._db_connection = apsw.Connection(':memory:')
        # We serialise writers during a write lock, and in normal cases the WAL mode avoids writers blocking
        # readers. Setting this is used to handle the one case in our normal operations that WAL mode requires
        # an exclusive lock for cleaning up the WAL file and associated shared-memory index.
        # See section 8 for the edge cases: https://www.sqlite.org/wal.html
        self._db_connection.setbusytimeout(1000)
        list(self._execute(cache_schema))
        self._db_connection.cursor().execute('begin immediate')
        self.doc_no = 0  # local only for this write transaction.
        self.frame_no = 0
        self.deleted_no = 0

    def commit(self):
        """Commit a transaction."""
        current_state = self._execute(prepare_flush, [self._db])
        max_document_id, max_frame_id, deleted_count = [row[0] for row in list(current_state)]
        self._execute(
            flush_cache,
            {
                'max_doc': max_document_id + 1,
                'max_frame': max_frame_id + 1,
                'deleted': self.deleted_no + deleted_count,
                'added': self.doc_no + max_document_id
            })
        self.doc_no = 0
        self.frame_no = 0
        self.deleted_no = 0

    def rollback(self):
        """Rollback a transaction on an IndexWriter."""
        self._execute('rollback')
        self.doc_no = 0
        self.frame_no = 0
        self.deleted_no = 0

    def close(self):
        """Close this storage object and all its resources, rendering it UNUSABLE."""
        self._db_connection.close()
        self._db_connection = None

    def add_structured_fields(self, field_names):
        """Register a structured field on the index. """
        for f in field_names:
            self._execute('insert into structured_field(name) values(?)', [f])

    def add_unstructured_fields(self, field_names):
        """Register an unstructured field on the index. """
        for f in field_names:
            self._execute('insert into unstructured_field(name) values(?)', [f])

    def delete_structured_fields(self, field_names):
        """Delete a structured field and the associated data from the index.

        Note this is a soft delete for the SqliteWriter class. Call
        :meth:SqliteWriter.materialize_deletes to remove all the data related
        to that field from the index. """
        raise NotImplementedError

    def delete_unstructured_fields(self, field_names):
        """Delete an unstructured field from the index.

        Note this is a soft delete for the SqliteWriter class. Call
        :meth:SqliteWriter.materialize_deletes to remove all the data related
        to that field from the index. """
        raise NotImplementedError

    def add_analyzed_document(self, document_format, document_data):
        """Add an analyzed document to the index.

        The added document will be assigned an integer document_id _after_ the commit method of this
        storage object runs to completion. These ID's are monotonically increasing and assigned in order
        of insertion.

        Arguments

            document_format: str
                A string representing the format of the passed data. Currently only 'test'
                is supported.
            document_data:
                The data for the document, in the format expected for document_data.

        Valid Document Formats

            document_format == 'test':
            An iterable of
                - a string representation of the whole document
                - a dictionary of field_name:field_value pairs for the document level structured data
                - a dictionary {
                    field_name: list of string representations of each frames
                }
                - a dictionary {
                    field_name: list of {term:frequency} vectors for each frame
                }
            For the frame data (3rd and 4th elements), the frames should be in document sequence order
            and there should be a one-one correspondence between frame representations and term:frequency vectors.

        """
        # TODO: pick a better specifier for the document format name here.
        if document_format == 'test':
            document, structured_data, frames, frame_terms = document_data

            # Stage the document.
            self._execute(
                'insert into document(id, stored) values (?, ?)',
                [self.doc_no, document]
            )

            # Stage the structured fields:
            insert_rows = ((self.doc_no, field, value) for field, value in structured_data.iteritems())
            self._executemany(
                'insert into document_data(document_id, field_name, value) values (?, ?, ?)',
                insert_rows
            )

            # Check frame data is consistent and pull out a frame count.
            number_frames = {field: len(values) for field, values in frames.iteritems()}
            number_frame_terms = {field: len(values) for field, values in frames.iteritems()}

            if number_frames.keys() != number_frame_terms.keys():
                raise ValueError('Inconsistent fields between frames and frame_terms')
            for field in number_frames:
                if number_frames[field] != number_frame_terms[field]:
                    raise ValueError('Number of frames and frame_terms does not match for field {}'.format(field))

            total_frames = sum(number_frames.values())

            # Stage the frames:
            insert_frames = (
                [self.doc_no, field, seq, frame]
                for field, frame_list in sorted(frames.iteritems())
                for seq, frame in enumerate(frame_list)
            )
            insert_frames_numbered = (
                (frame_count + self.frame_no, a[0], a[1], a[2], a[3])
                for frame_count, a in enumerate(insert_frames)
            )

            self._executemany(
                'insert into frame(id, document_id, field_name, sequence, stored) values (?, ?, ?, ?, ?)',
                insert_frames_numbered
            )

            # Term vectors for the frames, note that the dictionary is sorted for consistency with insert_frames
            frame_term_data = (frame for field, frame_list in sorted(frame_terms.iteritems()) for frame in frame_list)
            insert_term_data = (
                (frame_count + self.frame_no, term, frequency)
                for frame_count, frame_data in enumerate(frame_term_data)
                for term, frequency in frame_data.iteritems()
            )

            self._executemany(
                'insert into positions_staging(frame_id, term, frequency) values (?, ?, ?)',
                insert_term_data
            )

            self.frame_no += total_frames
            self.doc_no += 1
        else:
            raise ValueError('Unknown document_format {}'.format(document_format))

    def delete_documents(self, document_ids):
        """Delete a document with the given id from the index. """
        document_id_gen = ((document_id,) for document_id in document_ids)
        self._executemany('insert into deleted_document(id) values(?)', document_id_gen)
        self.deleted_no += len(document_ids)

    def set_plugin_state(self, plugin_type, plugin_settings, plugin_state):
        """ Set the plugin state in the index to the given state.

        Existing plugin state will be replaced.
        """

        # Insert into the plugin registry. If plugin_id already existed, reuse it.
        self._execute(
            "insert into plugin_registry(plugin_type, settings) values (?, ?); ",
            (plugin_type, plugin_settings)
        )

        insert_rows = ((plugin_type, plugin_settings, key, value) for key, value in plugin_state.iteritems())
        self._executemany("insert into plugin_data values (?, ?, ?, ?);", insert_rows)

    def delete_plugin_state(self, plugin_type, plugin_settings=None):
        """Delete a plugin instance, or all plugins of a certain type from the index. """
        self._execute('insert into delete_plugin values(?, ?)', (plugin_type, plugin_settings))

    def set_setting(self, name, value):
        """Set the setting ``name`` to ``value``"""
        self._execute('insert into setting values(?, ?)', [name, value])

    def _execute(self, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.execute(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise e

    def _executemany(self, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.executemany(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise e


class SqliteReader(StorageReader):
    """
    Abstract class used to read the contents of an index.

    Implementers must provide primitives for implementing atomic transactions. That is, they must provide
    :meth:`.begin`, :meth:`.commit`, and meth:`.rollback`.

    Storage implementations must also ensure that they provide reader/writer isolation. That is, if a storage instance
    is created and a transaction started, any write operations made in that transaction should not be visible to any
    existing or new storage instances. After the transaction is committed, the changes should not be visible to any
    existing instances that have called :meth:`.begin` but should be visible to any new or existing storage instances
    that are yet to call :meth:`.begin`.

    The :meth:`.__init__` method of a storage implementation should take care of the required bootstrap required to open
    existing storage **OR** create new storage (via a ``create`` flag). It also needs to support a ``readonly`` flag.

    Finally, storage instances **MUST** be thread-safe.

    """

    def __init__(self, path):
        """Open or create a reader for the given storage location."""
        self._db_path = path
        self._db = os.path.join(path, 'storage.db')

        if not os.path.exists(self._db):
            raise StorageNotFoundError('Can\'t find the resources required by SQLiteStorage. Is it corrupt?')

        self._db_connection = apsw.Connection(self._db, flags=apsw.SQLITE_OPEN_READONLY)

        self._db_connection.setbusytimeout(1000)

    def begin(self):
        """Begin a read transaction."""
        self._db_connection.cursor().execute('begin')

    def commit(self):
        """End the read transaction."""
        self._db_connection.cursor().execute('commit')
        return

    def close(self):
        """Close the reader, freeing up the database connection objects. """
        self._db_connection.close()
        self._db_connection = None

    def get_plugin_state(self, plugin_type, plugin_settings):
        """Return a dictionary of key-value pairs identifying that state of this plugin."""
        plugin_id = self._execute("select plugin_id from plugin_registry where plugin_type = ? and settings = ?",
                                  (plugin_type, plugin_settings)).fetchone()

        if plugin_id is None:
            raise PluginNotFoundError('Plugin not found in this index')

        else:
            plugin_state = self._execute("select key, value from plugin_data where plugin_id = ?;",
                                         plugin_id)
            for row in plugin_state:
                yield row

    def get_plugin_by_id(self, plugin_id):
        """Return the settings and state of the plugin identified by ID."""
        row = self._execute(
            'select plugin_type, settings from plugin_registry where plugin_id = ?', [plugin_id]
        ).fetchone()
        if row is None:
            raise PluginNotFoundError
        plugin_type, settings = row
        state = self._execute("select key, value from plugin_data where plugin_id = ?", [plugin_id]).fetchall()
        return plugin_type, settings, state

    def list_known_plugins(self):
        """ Return a list of (plugin_type, settings, id) triples for each plugin stored in the index. """
        return [row for row in self._execute("select plugin_type, settings, plugin_id from plugin_registry;")
                if row is not None]

    @property
    def structured_fields(self):
        """Get a list of the structured field names on this index."""
        rows = list(self._execute('select name from structured_field'))
        return [row[0] for row in rows]

    @property
    def unstructured_fields(self):
        """Get a list of the unstructured field names on this index."""
        rows = list(self._execute('select name from unstructured_field'))
        return [row[0] for row in rows]

    def vocabulary_count(self, include_fields=None, exclude_fields=None):
        """Return the number of unique terms occuring in the given combinations of fields. """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        vocab_size = self._execute(
            'select count(distinct term_id) '
            'from term_statistics stats '
            'inner join unstructured_field field '
            '    on stats.field_id = field.id ' + where_clause,
            fields
        ).fetchone()
        return vocab_size[0]

    def get_frequencies(self, include_fields=None, exclude_fields=None):
        """Return a generator of all the term frequencies in the given fields."""
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        frequencies = self._execute(
            'select voc.term, sum(frames_occuring)'
            'from term_statistics stats '
            'inner join vocabulary voc '
            '   on voc.id = stats.term_id '
            'inner join unstructured_field field '
            '   on stats.field_id = field.id ' + where_clause +
            'group by voc.term', fields
        )
        return frequencies

    def get_term_frequencies(self, terms, include_fields=None, exclude_fields=None):
        """Return a generator of frequencies over the list of terms supplied. """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        term_parameters = ', '.join(['?'] * len(terms))

        frequencies = self._execute(
            'select voc.term, sum(frames_occuring)'
            'from term_statistics stats '
            'inner join vocabulary voc '
            '   on voc.id = stats.term_id '
            'inner join unstructured_field field '
            '   on stats.field_id = field.id ' + where_clause +
            '   and voc.term in ({})'.format(term_parameters) +
            'group by voc.term', fields + terms
        )
        return frequencies

    def get_positions_index(self, include_fields=None, exclude_fields=None):
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

    def get_term_positions(self, term, include_fields=None, exclude_fields=None):
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

    def get_frame(self, frame_id, field):
        """Fetch frame ``frame_id`` (str)."""
        return json.loads(self.__storage.get_container_item(IndexWriter.FRAMES_CONTAINER.format(field), frame_id))

    def get_frames(self, frame_ids=None,):
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

    def get_frame_ids(self, include_fields=None, exclude_fields=None):
        """Generator of ids for all frames stored on this index."""
        for f_id in self.__storage.get_container_keys(IndexWriter.FRAMES_CONTAINER.format(field)):
            yield f_id

    def count_documents(self):
        """Returns the number of documents in the index."""
        return self._execute('select count(*) from document').fetchone()[0]

    def count_frames(self, include_fields=None, exclude_fields=None):
        """Returns the number of documents in the index."""
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)
        return self._execute(
            'select count(*) from frame '
            'inner join unstructured_field field '
            '   on field.id = frame.field_id ' + where_clause,
            fields
        ).fetchone()[0]

    def iterate_documents(self):
        """Returns a generator  of (document_id, stored_document) pairs for the entire index.

        The generator will only be valid as long as this reader is open.

        """
        return self._execute('select * from document')

    def iterate_frames(self, include_fields=None, exclude_fields=None):
        """Returns a generator  of (frame_id, document_id, field, sequence, stored_frame) tuples
         for the specified unstructured fields in the index.

        The generator will only be valid as long as this reader is open.

        """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)
        return self._execute(
            'select frame.id, document_id, field.name, sequence, stored '
            'from frame '
            'inner join unstructured_field field '
            '   on field.id = frame.field_id ' + where_clause,
            fields
        )

    def get_document(self, document_id):
        """Return the document with the given document_id. """
        row = list(self._execute('select stored from document where id = ?', [document_id]))
        if row is None:
            raise KeyError('Document {} not found'.format(document_id))
        else:
            return row[0][0]

    def get_documents(self, document_ids=None):
        """
        Generator that yields documents from this index as (id, data) tuples.

        If present, the returned documents will be restricted to those with ids in ``document_ids`` (list).

        """
        for k, v in self.__storage.get_container_items(IndexWriter.DOCUMENTS_CONTAINER, keys=document_ids):
            yield (k, json.loads(v))

    def get_metadata(self, include_fields=None, exclude_fields=None, frames=True):
        """
        Get the metadata index.

        This method is a generator that yields tuples (field_name, value, [frame_ids])

        The frames flag indicates whether the return values should be broadcast to frame_ids (default)
        or document_ids.

        """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields, structured=True)
        if frames:
            rows = self._execute(
                'select field.name, value, frame.id '
                'from document_data '
                'inner join frame '
                '   on frame.document_id = document_data.document_id '
                'inner join structured_field field '
                '   on field.id = document_data.field_id ' + where_clause +
                'order by document_data.field_id, value',
                fields
            )
        else:
            rows = self._execute(
                'select field.name, value, document_id '
                'from document_data '
                'inner join structured_field field '
                '   on field.id = document_data.field_id ' + where_clause +
                'order by field_id, value',
                fields
            )
        current_field, current_value, document_id = next(rows)
        document_ids = [document_id]
        for row in rows:
            field, value, document_id = row
            # Rows are sorted by field, value, so as soon as the change can yield
            if field == current_field and value == current_value:
                document_ids.append(document_id)
            else:
                yield current_field, current_value, document_ids
                current_field = field
                current_value = value
                document_ids = [document_id]
        else:  # Make sure to yield the final row.
            yield current_field, current_value, document_ids

    def get_settings(self, names):
        """Get the settings identified by the given names. """
        variable_binding = ', '.join(['?'] * len(names))
        return self._execute(
            'select * from setting where name in ({})'.format(variable_binding),
            names
        )

    @property
    def revision(self):
        """
        The revision identifier is a tuple (revision_number, added_documents, deleted_documents)

        The revision number is incremented on every writer commit, the added documents and deleted
        documents count the number of times add_analyzed_document and delete_document were
        succesfully called on a writer for this index.
        """
        revision = self._execute(
            'select * from index_revision where revision_number=(select max(revision_number) from index_revision)'
        ).fetchone()
        return revision

    def _execute(self, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.execute(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise e

    def _executemany(self, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.executemany(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise e

    def _fielded_where_clause(self, include_fields, exclude_fields, structured=False):
        """Generate a where clause for field inclusion, validating the fields at the same time.

        Include fields takes priority if both include and exclude fields are specified.

        Returns both the where clause and the list of fields to filter on for binding.

        """
        fields = include_fields or exclude_fields or []
        valid_fields = self.structured_fields if structured else self.unstructured_fields
        invalid_fields = [field for field in fields if field not in valid_fields]

        if invalid_fields:
            raise ValueError('Invalid fields: {} do not exist or are not indexed'.format(invalid_fields))
        if include_fields:
            where_clause = 'where field.name in ({})'.format(', '.join(['?'] * len(include_fields)))
        elif exclude_fields:
            where_clause = 'where field.name not in ({})'.format(', '.join(['?'] * len(exclude_fields)))
        else:
            where_clause = ''
        return where_clause, fields

    @staticmethod
    def _chunks(l, n=999):
        """Yield successive n-sized chunks from l."""
        l = list(l)
        for i in xrange(0, len(l), n):
            yield l[i:i + n]


SqliteStorage = Storage(SqliteReader, SqliteWriter)
