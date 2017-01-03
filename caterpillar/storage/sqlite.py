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

import ujson as json
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
            # The database is never written to directly, hence it is closed after initialisation.
            list(cursor.execute(disk_schema))
            connection.close()

    def begin(self):
        """
        Begin a transaction.

        A temporary in-memory database is created to cache results, which is destroyed after the
        commit or rollback methods are called.

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
        self.doc_no = 0  # local only for this write transaction.
        self.frame_no = 0
        self.committed = False
        self._flushed = False

    def commit(self):
        """Commit a transaction.

        First the on disk database is attached and the current maximum document and frame ID's are returned.

        Then the content of the in memory cache is flushed to the database. The cache is then dropped. The begin
        method will need to be called before this method is usable for writing again.

        Returns a list of the added documents.

        """
        if not self._flushed:
            self._flush()
        self._execute('commit; detach database disk_index;')
        self.committed = True
        self.doc_no = 0
        self.frame_no = 0

        return self.__last_added_documents, self.__deleted_documents

    def rollback(self):
        """Rollback a transaction on an IndexWriter."""
        self._execute('rollback')
        self.doc_no = 0
        self.frame_no = 0

    def close(self):
        """
        Close this storage object and all its resources, rendering it UNUSABLE.

        This operates immediately: if the data has not been committed it will be destroyed.

        """
        # If we have committed, the resources have already been released.
        if not self.committed:
            self._db_connection.close()
            self._db_connection = None

    def _prepare_flush(self):
        """Prepare to flush the cached data to the index.

        This method returns the state of the index for synchronising document counts and a list of ID's
        for deleted documents.

        """
        return list(self._execute(prepare_flush, [self._db]))

    def _flush(self):
        """Actually perform the flush."""
        index_sync_data = self._prepare_flush()
        revision, max_document_id, deleted_count, max_frame_id = index_sync_data[0]
        self.__deleted_documents = index_sync_data[1:]
        self.__last_added_documents = list(range(max_document_id + 1, max_document_id + 1 + self.doc_no))
        self._execute(
            flush_cache,
            {
                'max_doc': max_document_id + 1,
                'max_frame': max_frame_id + 1,
                'deleted': deleted_count + len(self.__deleted_documents),
                'added': self.doc_no + max_document_id,
                'added_frames': self.frame_no + max_frame_id
            })
        self._flushed = True  # Only needed for _merge_term_variants currently.

    def add_structured_fields(self, field_names):
        """Register a structured field on the index. """
        for f in field_names:
            self._execute('insert into structured_field(name) values(?)', [f])

    def _merge_term_variation(self, old_term, new_term, field):
        """Mangle the terms in the stored vocabulary.

        term_mapping is a list of pairs ('old term', 'new term'). The old_term mapping can
        itself be a tuple, in which case all individual terms in old_term are mapped to
        'new_term'.

        If new term is falsey, that term representation will be removed from the vocabulary.

        If old_term is not present in the vocabulary, it will be ignored.

        Note that this operation operates only on content that has been commited to disk already:
        this function must be called in a write transaction *after* the documents to operate on
        have been added.

        This is a destructive operation that does not respect the writer interface and should be
        used with care. In memory contents are first flushed to disk, and commit is executed after
        this operation.

        """
        # Flush only if necessary, as this method needs to work on the on disk representation.
        if not self._flushed:
            self._flush()

        # Make sure new term is in the vocabulary, then stage all the old and new terms for merging
        self._execute("""
            insert or ignore into disk_index.vocabulary(term) values(:new_term);

            insert into term_merging
                select (select id from disk_index.vocabulary where term = :new_term) as term_id,
                    frame_id,
                    sum(frequency),
                    group_concat(positions, ',') -- Concatenate the JSON strings together.
                from disk_index.term_posting post
                inner join disk_index.frame
                    on post.frame_id = frame.id
                inner join disk_index.unstructured_field field
                    on frame.field_id = field.id
                where term_id in (select id from disk_index.vocabulary where term in (:old_term, :new_term))
                    and field.name = :field
                group by frame_id;

            delete from disk_index.term_posting
                where term_id in (select id from disk_index.vocabulary where term in (:old_term, :new_term))
                    and frame_id in (select distinct frame_id from term_merging);

            delete from disk_index.frame_posting
                where term_id in (select id from disk_index.vocabulary where term in (:old_term, :new_term))
                    and frame_id in (select distinct frame_id from term_merging);

            delete from disk_index.term_statistics
                where term_id in (select id from disk_index.vocabulary where term = :old_term)
                    and field_id = (select id from disk_index.unstructured_field where name = :field);

            insert or replace into disk_index.term_statistics
                select term_id,
                       (select id from disk_index.unstructured_field where name = :field),
                       sum(frequency),
                       count(frame_id),
                       0 -- document count not currently fully implemented
                from term_merging;

            insert into disk_index.term_posting
                select * from term_merging;

            insert into disk_index.frame_posting(term_id, frame_id, frequency, positions)
                select * from term_merging;

            delete from term_merging;

            """, {'new_term': new_term, 'old_term': old_term, 'field': field}
        )

    def _merge_bigrams(self, left_term, right_term, bigram, field, max_char_gap=2):
        """
        Merge adjacent occurences of left_term and right_term into the bigram phrase.

        """
        # Flush only if necessary, as this method needs to work on the on disk representation.
        if not self._flushed:
            self._flush()

        bigram_rows = self._execute("""
            insert or ignore into disk_index.vocabulary(term) values(:bigram);

            -- Select rows for frames where both left term and right term occur.
            with left as (
                select
                    term_id as left_term,
                    frame_id,
                    positions as left_positions,
                    frequency as left_frequency
                from disk_index.term_posting post
                inner join disk_index.frame
                    on post.frame_id = frame.id
                where term_id = (select id from disk_index.vocabulary where term = :left_term)
                    and frame.field_id = (select id from disk_index.unstructured_field where name = :field)
            ),
            right as (
                select
                    term_id as right_term,
                    frame_id,
                    positions as right_positions,
                    frequency as right_frequency
                from disk_index.term_posting post
                inner join disk_index.frame
                    on post.frame_id = frame.id
                where term_id = (select id from disk_index.vocabulary where term = :right_term)
                    and frame.field_id = (select id from disk_index.unstructured_field where name = :field)
            )
            insert into bigram_staging
                select
                    left.frame_id,
                    (select id from vocabulary where term = :bigram) as new_term_id,
                    left_term,
                    left_positions,
                    left_frequency,
                    right_term,
                    right_positions,
                    right_frequency
                from left
                inner join right
                    on left.frame_id = right.frame_id;

            delete from disk_index.term_posting
                where term_id in (select id from disk_index.vocabulary where term in (:left_term, :right_term))
                    and frame_id in (select distinct frame_id from bigram_staging);

            delete from disk_index.frame_posting
                where term_id in (select id from disk_index.vocabulary where term in (:left_term, :right_term))
                    and frame_id in (select distinct frame_id from bigram_staging);

            -- Select out the rows to merge the positions of
            select * from bigram_staging;

            """, {'left_term': left_term, 'right_term': right_term, 'bigram': bigram, 'field': field}
        )

        def insert_row(values):
            self._execute('insert into bigram_merging values(?, ?, ?, ?)', values)

        for frame_id, bigram_id, left_id, left_positions, _, right_id, right_positions, _ in bigram_rows:

            if left_id != right_id:  # Different left and right terms
                left = [(start, end, 0) for start, end in json.loads('[{}]'.format(left_positions))]
                right = [(start, end, 1) for start, end in json.loads('[{}]'.format(right_positions))]
                sorted_positions = sorted(left + right)

                # Sort positions, then merge those that match the sequence
                merged_positions = []
                final_left = []
                final_right = []

                merged = False
                for first, second in zip(sorted_positions, sorted_positions[1:]):
                    if merged:  # second term has been consumed, so move along
                        merged = False
                        continue
                    # It's a bigram if the left term is follwed by the right term, and they are close enough.
                    if (
                        first[2] == 0 and
                        second[2] == 1 and
                        # Needs to be greater than zero to handle repeating terms.
                        0 < second[0] - first[1] <= max_char_gap
                    ):
                        merged_positions.append([first[0], second[1]])
                        merged = True
                    # Otherwise it's not, and we just keep the positions associated with that term.
                    else:
                        if first[2] == 0:
                            final_left.append(first[:2])
                        elif first[:2] != second[:2]:  # Avoid adding a repeated term twice.
                            final_right.append(first[:2])
                else:  # Don't forget the last position if it wasn't merged.
                    if not merged:
                        if second[2] == 0:
                            final_left.append(second[:2])
                        else:  # Avoid adding a repeated term twice.
                            final_right.append(second[:2])

                if final_left:
                    insert_row([left_id, frame_id, len(final_left), json.dumps(final_left)[1:-1]])
                if final_right and left_id != right_id:  # Handle terms with repeated
                    insert_row([right_id, frame_id, len(final_right), json.dumps(final_right)[1:-1]])
                if merged_positions:
                    insert_row([bigram_id, frame_id, len(merged_positions), json.dumps(merged_positions)[1:-1]])

            else:  # Special case for the same term occuring in a row.
                positions = sorted([(start, end, 0) for start, end in json.loads('[{}]'.format(left_positions))])

                merged_positions = []
                final = []
                merged = False

                for first, second in zip(positions, positions[1:]):
                    if merged:  # second term has been consumed, so move along
                        merged = False
                        continue
                    if 0 < second[0] - first[1] <= max_char_gap:
                        merged_positions.append([first[0], second[1]])
                        merged = True
                    else:
                        final.append(first[:2])
                else:  # Don't forget the last position if it wasn't merged.
                    if not merged:
                        final.append(second[:2])

                if final:
                    insert_row([left_id, frame_id, len(final), json.dumps(final)[1:-1]])
                if merged_positions:
                    insert_row([bigram_id, frame_id, len(merged_positions), json.dumps(merged_positions)[1:-1]])

        # Finally, insert all the new values and update the term_statistics
        self._execute("""
            insert into disk_index.frame_posting(term_id, frame_id, frequency, positions)
                select *
                from bigram_merging;

            insert into disk_index.term_posting(term_id, frame_id, frequency, positions)
                select *
                from bigram_merging;

            insert or replace into disk_index.term_statistics(term_id, field_id, frequency, frames_occuring)
                select
                    term_id,
                    field_id,
                    sum(frequency) as frequency,
                    count(frame_id) as frames_occuring
                from disk_index.term_posting post
                inner join disk_index.frame
                    on frame.id = post.frame_id
                where frame.field_id in (select id from disk_index.unstructured_field where name = :field)
                    and term_id in (
                        select id from disk_index.vocabulary where term in (:left_term, :right_term, :bigram)
                    )
                group by term_id, field_id;

            delete from bigram_staging;
            delete from bigram_merging;
        """, {'left_term': left_term, 'right_term': right_term, 'bigram': bigram, 'field': field}
        )

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
                    field_name: list of {term: [[word1 boundary], [word2 boundary]]} vectors for each frame
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
                # Strip the leading and trailing '[' from the json string so aggregation becomes string concatenation.
                (frame_count + self.frame_no, term, len(positions), json.dumps(positions)[1:-1])
                for frame_count, frame_data in enumerate(frame_term_data)
                for term, positions in frame_data.iteritems()
            )

            self._executemany(
                'insert into positions_staging(frame_id, term, frequency, positions) values (?, ?, ?, ?)',
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
        plugin_id = list(
            self._execute(
                "select plugin_id from plugin_registry where plugin_type = ? and settings = ?",
                (plugin_type, plugin_settings)
            )
        )

        if not plugin_id:
            raise PluginNotFoundError('Plugin not found in this index')

        else:
            plugin_state = self._execute(
                "select key, value from plugin_data where plugin_id = ?;",
                plugin_id[0]
            )

            for row in plugin_state:
                yield row

    def get_plugin_by_id(self, plugin_id):
        """Return the settings and state of the plugin identified by ID."""
        row = list(self._execute(
            'select plugin_type, settings from plugin_registry where plugin_id = ?', [plugin_id]
        ))
        if not row:
            raise PluginNotFoundError
        plugin_type, settings = row[0]
        state = self._execute("select key, value from plugin_data where plugin_id = ?", [plugin_id]).fetchall()
        return plugin_type, settings, state

    def list_known_plugins(self):
        """ Return a list of (plugin_type, settings, id) triples for each plugin stored in the index. """
        return list(self._execute("select plugin_type, settings, plugin_id from plugin_registry;"))

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

    def count_vocabulary(self, include_fields=None, exclude_fields=None):
        """Return the number of unique terms occuring in the given combinations of fields. """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        vocab_size = list(self._execute(
            'select count(distinct term_id) '
            'from term_statistics stats '
            'inner join unstructured_field field '
            '    on stats.field_id = field.id ' + where_clause,
            fields
        ))
        return vocab_size[0][0]

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

    def _iterate_positions(self, terms=None, include_fields=None, exclude_fields=None):
        """Iterate through the positions array for the given term.

        Currently this relies on the _positions field being stored in the frame, and is not
        easily accessible otherwise. This is provided as a backwards compatability measure,
        not a robust implementation of character level position information.

        If you just want term-frequency information, the iterate_... provides the ...
        TODO: implement and point to the new iterators for term-frequency information.
        TODO: implement and point at the low level search operators.

        """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        if terms is None:
            terms = (row[0] for row in self._execute(
                """
                select distinct term
                from term_statistics ts
                inner join vocabulary v
                    on ts.term_id = v.id
                inner join unstructured_field field
                    on field.id = ts.field_id
                {}
                """.format(where_clause),
                fields
            ))

        if fields:
            where_clause += 'and term = ?'
        else:
            where_clause = 'where term = ?'

        data = (fields + [term] for term in terms)
        frames = self._executemany(
            """
            select vocab.term, frame.id, field.name, post.positions
            from term_posting post
            inner join vocabulary vocab
                on vocab.id = post.term_id
            inner join frame
                on frame.id = post.frame_id
            inner join unstructured_field field
                on field.id = frame.field_id
            {}
            order by term, frame.id
            """.format(where_clause),
            data
        )

        current_term = None

        for term, frame_id, field_name, term_positions in frames:
            if current_term is None:
                positions = {frame_id: sorted(json.loads('[{}]'.format(term_positions)))}
                current_term = term
            elif term == current_term:
                positions[frame_id] = sorted(json.loads('[{}]'.format(term_positions)))
            else:
                yield current_term, positions
                positions = {frame_id: sorted(json.loads('[{}]'.format(term_positions)))}
                current_term = term
        else:
            if current_term is not None:
                yield current_term, positions

    def iterate_associations(self, term=None, association=None, include_fields=None, exclude_fields=None):
        """
        Term associations for this Index.

        This is used to record when two terms co-occur in a frame. Be aware that only 1 co-occurrence for two terms
        is recorded per frame no matter the frequency of each term. The format is as follows::

            {
                term: {
                    other_term: count,
                    ...
                },
                ...
            }

        Optionally the term or both the term and the association may be supplied.

        This method is a generator which yields a dict of {other_term: count, ...} for every term in the index.

        Note that this is dynamically calculated and may be expensive for large indexes.

        """

        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        # If no field selectivity is defined, we can leave off the two joins:
        if fields:
            joined_where = """
                inner join frame
                    on frame.id = frame_post.frame_id
                inner join unstructured_field field
                    on field.id = frame.field_id
                {}""".format(where_clause)
        else:
            joined_where = ''

        # If a term is specified, add it to the join condition.
        term_filter = 'and left_vocab.term = ?' if term is not None else ''
        terms = [term] if term is not None else []
        association_filter = 'and right_vocab.term = ?' if association is not None else ''
        associations = [association] if association is not None else []

        rows = self._execute(
            """
            select
                left_vocab.term,
                right_vocab.term,
                count(*)
            from term_posting term_post
            inner join frame_posting frame_post
                on term_post.frame_id = frame_post.frame_id
                and term_post.term_id != frame_post.term_id
            inner join vocabulary left_vocab
                on left_vocab.id = term_post.term_id
                {}
            inner join vocabulary right_vocab
                on right_vocab.id = frame_post.term_id
                {}

            {}

            group by left_vocab.term, right_vocab.term
            order by left_vocab.term """.format(term_filter, association_filter, joined_where),
            terms + associations + fields
        )

        current_term = None

        for term, other_term, count in rows:
            if current_term is None:
                current_dict = {other_term: count}
                current_term = term

            elif current_term == term:
                current_dict[other_term] = count

            else:
                yield current_term, current_dict
                current_term = term
                current_dict = {other_term: count}

        else:  # Make sure to yield the final row.
            if current_term is not None:
                yield current_term, current_dict

    def count_documents(self):
        """Returns the number of documents in the index."""
        return list(self._execute('select count(*) from document'))[0][0]

    def count_frames(self, include_fields=None, exclude_fields=None):
        """Returns the number of documents in the index."""
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)
        return list(self._execute(
            'select count(*) from frame '
            'inner join unstructured_field field '
            '   on field.id = frame.field_id ' + where_clause,
            fields
        ))[0][0]

    def iterate_documents(self, document_ids=None):
        """Returns a generator  of (document_id, stored_document) pairs for the entire index.

        Optionally specify a list of document_ids to iterate over.

        The generator will only be valid as long as this reader is open.

        """
        if document_ids is not None:
            return self._executemany(
                'select * from document where id = ?',
                [[document_id] for document_id in document_ids]
            )
        else:
            return self._execute('select * from document')

    def iterate_frames(self, include_fields=None, exclude_fields=None, frame_ids=None):
        """Returns a generator  of (frame_id, document_id, field, sequence, stored_frame) tuples
        for the specified unstructured fields in the index.

        Optionally specify a list of frame_ids, in which case the field arguments will be ignored.

        The generator will only be valid as long as this reader is open.

        """
        if frame_ids is not None:
            return self._executemany(
                'select frame.id, document_id, field.name, sequence, stored '
                'from frame '
                'inner join unstructured_field field '
                '   on field.id = frame.field_id '
                'where frame.id = ?', [[frame_id] for frame_id in frame_ids]
            )
        else:
            where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)
            return self._execute(
                'select frame.id, document_id, field.name, sequence, stored '
                'from frame '
                'inner join unstructured_field field '
                '   on field.id = frame.field_id ' + where_clause,
                fields
            )

    def iterate_metadata(self, include_fields=None, exclude_fields=None, frames=True, text_field=None):
        """
        Get the metadata index.

        This method is a generator that yields tuples (field_name, value, [frame_ids])

        The frames flag indicates whether the return values should be broadcast to frame_ids (default)
        or document_ids.

        The optional text_field specifier allow filtering frames to just the included text field.

        """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields, structured=True)

        if text_field and fields:
            where_clause += ' and text_field.name = ?'
            fields += [text_field]
        elif text_field:
            where_clause = ' where text_field.name = ?'
            fields += [text_field]

        if frames:
            rows = self._execute(
                'select field.name, value, frame.id '
                'from document_data '
                'inner join frame '
                '   on frame.document_id = document_data.document_id '
                'inner join structured_field field '
                '   on field.id = document_data.field_id '
                'inner join unstructured_field text_field '
                '   on text_field.id = frame.field_id ' + where_clause +
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
        revision = list(self._execute(
            'select * from index_revision where revision_number=(select max(revision_number) from index_revision)'
        ))
        return revision[0]

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


SqliteStorage = Storage(SqliteReader, SqliteWriter)
