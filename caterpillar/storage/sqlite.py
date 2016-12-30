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
        self.deleted_no = 0
        self.committed = False

    def commit(self):
        """Commit a transaction.

        First the on disk database is attached and the current maximum document and frame ID's are returned.

        Then the content of the in memory cache is flushed to the database. The cache is then dropped. The begin
        method will need to be called before this method is usable for writing again.

        """
        max_document_id, max_frame_id, deleted_count = self._prepare_flush()
        self._flush(max_document_id, max_frame_id, deleted_count)
        self.committed = True
        self._db_connection.close()
        self._db_connection = None
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
        """
        Close this storage object and all its resources, rendering it UNUSABLE.

        This operates immediately: if the data has not been committed it will be destroyed.

        """
        # If we have committed, the resources have already been released.
        if not self.committed:
            self._db_connection.close()
            self._db_connection = None

    def _prepare_flush(self):
        """Prepare to flush the cached data to the index. """
        current_state = [row[0] for row in self._execute(prepare_flush, [self._db])]
        return current_state

    def _flush(self, max_document_id, max_frame_id, deleted_count):
        """Actually perform the flush."""
        self._execute(
            flush_cache,
            {
                'max_doc': max_document_id + 1,
                'max_frame': max_frame_id + 1,
                'deleted': self.deleted_no + deleted_count,
                'added': self.doc_no + max_document_id
            })

    def add_structured_fields(self, field_names):
        """Register a structured field on the index. """
        for f in field_names:
            self._execute('insert into structured_field(name) values(?)', [f])

    def _mangle_terms(self, term_mapping):
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
        used with care.

        """
        # Unwrap term mapping to a more friendly dictionary, accounting for the different data
        # structure in the merging unigrams to an n-gram case.
        uni_mapping = {}
        for old_term, new_term in term_mapping:
            if isinstance(old_term, basestring):
                uni_mapping[old_term] = new_term
            else:
                raise ValueError('_mangle_terms can only remap single terms - use _mangle_phrases instead')

        self._executemany(
            'insert into vocabulary_mangle values(?, ?)',
            uni_mapping.iteritems()
        )

        # TODO: clean up all the temporary tables so consecutive runs don't get confused.
        # Handle unigram case first: identify and merge all the matching positions.
        change_unigrams = self._execute("""
            commit; attach database ? as disk_index; begin;

            -- Update the vocabulary with any new values:
            insert into disk_index.vocabulary(term)
                select distinct new_term
                from vocabulary_mangle mangle
                where not exists (select 1
                                 from disk_index.vocabulary
                                 where term = new_term);

            create table vocab_map as
                select distinct mangle.*, vocab.id as old_id, new_vocab.id as new_id
                from vocabulary_mangle mangle
                inner join disk_index.vocabulary vocab
                    on mangle.old_term = vocab.term
                inner join disk_index.vocabulary new_vocab
                    on mangle.new_term = new_vocab.term;

            -- Select all instances of old and new terms for updating statistics.
            -- One row for each old term and new term, combining them if they occur in the same frame
            with old_posting as (
                select frame_id, frequency, positions, new_id as term_id
                from disk_index.term_posting
                inner join vocab_map
                    vocab_map.old_id = term_posting.term_id
            ),
            new_posting as (
                select frame_id, frequency, positions, new_id as term_id
                from disk_index.term_posting
                inner join vocab_map
                    vocab_map.new_id = term_posting.term_id
            )
            create table to_mangle_posting as
                select frame_id, term_id,
                    coalesce(old_frequency, 0) + coalesce(new_frequency, 0) as frequency,
                    coalesce(old_postings, '[]') as old_postings,
                    coalesce(new_postings, '[]') as new_postings
                from ( -- Simulates a full outer join
                    select
                        old.frame_id as frame_id,
                        new.term_id as term_id,
                        old.frequency as old_frequency,
                        old.positions as old_positions,
                        new.frequency as new_frequency,
                        new.positions as new_positions
                    from old_posting old
                    left outer join new_posting new
                        on old.term_id = new.term_id
                        and old.frame_id = new.frame_id
                    union
                    select
                        new.frame_id as frame_id,
                        new.term_id as term_id,
                        old.frequency as old_frequency,
                        old.positions as old_positions,
                        new.frequency as new_frequency,
                        new.positions as new_positions
                    from new_posting new
                    left outer join old_posting old
                        on old.term_id = new.term_id
                        and old.frame_id = new.frame_id
                );

            -- Clear out old values from tables
            delete from disk_index.term_posting
                where term_id in (select old_id from vocab_map union select new_id from vocab_map);

            delete from disk_index.frame_posting
                where term_id in (select old_id from vocab_map union select new_id from vocab_map);

            delete from disk_index.term_statistics
                where term_id in (select old_id from vocab_map union select new_id from vocab_map);

            -- Update the term_statistics:
            insert or replace into disk_index.term_statistics(term_id, field_id, frequency, frames_occuring)
                select term_id, field_id, sum(frequency), count(distinct frame_id)
                from to_mangle_posting
                inner join disk_index.frame
                    on to_mangle_posting.frame_id = frame.id
                group by term_id, field_id;

            -- Return the rows to have their postings merged
            select *
            from to_mangle_posting
            order by frame_id, term_id;

            """, [self._db]
        )

        def insert_row(values):
            self._execute(
                """
                insert into disk_index.term_posting(frame_id, term_id, frequency, positions)
                    values(?, ?, ?, ?);
                insert into disk_index.frame_posting(frame_id, term_id, frequency, positions)
                    values(?, ?, ?, ?);
                """, values * 2
            )

        # Aggregate positions arrays

        for row in change_unigrams:
            frame_id, term_id, frequency, new_positions, old_positions = row

            if old_positions is not None:
                positions = json.dumps(json.loads(new_positions) + json.loads(old_positions))
            else:
                positions = new_positions

            insert_row([frame_id, term_id, frequency, positions])

        self._execute('commit; detach disk_index; begin;')

    def _mangle_phrases(self, bigram_frame_positions):
        """Materialise the bigrams occuring in the identified positions of the given frames.

        Replaces the unigram element of the term.

        """

        mangle_rows = (
            (bigram, bigram.split(' ')[0], bigram.split(' ')[1], frame_id, len(positions), json.dumps(positions))
            for bigram, frames in bigram_frame_positions
            for frame_id, positions in frames.iteritems()
        )

        self._executemany(
            'insert into phrase_mangle values(?, ?, ?, ?, ?, ?)',
            mangle_rows
        )

        change_bigrams = self._execute("""
            commit; attach database ? as disk_index; begin;

            -- Update the vocabulary with any new values:
            insert into disk_index.vocabulary(term)
                select distinct bigram
                from phrase_mangle mangle
                where not exists (select 1
                                  from disk_index.vocabulary
                                  where term = bigram);

            -- Temporary mapping table for vocabulary id's
            create table phrase_map as
                select distinct
                    bigram, left_unigram, right_unigram,
                    bi_vocab.id as bigram_id,
                    left_vocab.id as left_id,
                    right_vocab.id as right_id
                from phrase_mangle mangle
                inner join disk_index.vocabulary bi_vocab
                    on mangle.bigram = bi_vocab.term
                inner join disk_index.vocabulary left_vocab
                    on mangle.left_unigram = left_vocab.term
                inner join disk_index.vocabulary right_vocab
                    on mangle.right_unigram = right_vocab.term;

            -- Horrible temporary table for splitting term postings where bigrams consume unigrams.
            -- This is one row per bigram, merging the left and right unigram postings into a single table.
            create table to_mangle_phrase as
                select
                    phrase_mangle.frame_id,
                    phrase_mangle.frequency as bigram_frequency,
                    phrase_mangle.positions as bigram_positions,
                    phrase_map.bigram_id,
                    phrase_map.left_id,
                    left_posting.frequency as left_frequency,
                    left_posting.positions as left_positions,
                    phrase_map.right_id,
                    right_posting.frequency as right_frequency,
                    right_posting.positions as right_positions
                from phrase_mangle
                inner join phrase_map
                    on phrase_mangle.bigram = phrase_map.bigram
                inner join disk_index.term_posting left_posting
                    on phrase_map.left_id = left_posting.term_id
                inner join disk_index.term_posting right_posting
                    on phrase_map.right_id = right_posting.term_id;

            -- For updating term statistics after all the mangling is done.
            create table old_unigram_posting as
                select term_posting.*
                from disk_index.term_posting
                inner join phrase_map
                    on phrase_map.left_id = term_posting.term_id
                    or phrase_map.right_id = term_posting.term_id
                where frame_id in (select distinct frame_id from phrase_mangle);

            -- Clear out old values from tables on disk.
            delete from disk_index.term_posting
            where term_id in (select left_id from phrase_map union select right_id from phrase_map)
                and frame_id in (select distinct frame_id from phrase_mangle);

            delete from disk_index.frame_posting
            where frame_id in (select distinct frame_id from phrase_mangle)
                and term_id in (select left_id from phrase_map union select right_id from phrase_map);

            -- Return the rows to have their postings merged
            select * from to_mangle_phrase;

            """, [self._db]
        )

        def insert_split_rows(row):
            def insert_row(values):
                self._execute(
                    """
                    insert into update_term_posting(frame_id, term_id, frequency, positions)
                        values(?, ?, ?, ?);
                    """, values
                )

            def merge_positions(bigram_positions, unigram_positions):
                valid_positions = []
                bigram_positions = json.loads(bigram_positions)
                for uni_position in json.loads(unigram_positions):
                    for bigram_position in bigram_positions:
                        # Remove the unigram position if it's within the bigram position.
                        if bigram_position[0] <= uni_position[0] <= bigram_position[1]:
                            continue
                        valid_positions.append(uni_position)
                return json.dumps(valid_positions)

            bigram_frequency, left_frequency, right_frequency = row[1], row[5], row[8]
            bigram_positions, left_positions, right_positions = row[2], row[6], row[9]
            # Always insert the bigram information:
            insert_row([row[0], row[3], bigram_frequency, bigram_positions])

            # Some instances of left term that are not part of the bigram
            if bigram_frequency < left_frequency:
                left_frequency -= bigram_frequency
                left_positions = merge_positions(bigram_positions, left_positions)
                insert_row([row[0], row[4], left_frequency, left_positions])
            # Instances of the right term that are not part of the bigram.
            elif bigram_frequency < right_frequency:
                right_frequency -= bigram_frequency
                right_positions = merge_positions(bigram_positions, right_positions)
                insert_row([row[0], row[4], right_frequency, right_positions])

        # Aggregate or split positions arrays.
        row = change_bigrams.fetchone()
        if row is not None:
            insert_split_rows(row)
            for row in change_bigrams:
                insert_split_rows(row)

        # Finally update the term statistics and flush everything to disk
        self._execute("""
            -- Update the term_statistics:
            with changed_terms as (
                select term_id, frame.field_id, sum(frequency) as frequency, count(distinct frame_id) as frames_occuring
                from update_term_posting post
                inner join disk_index.frame
                    on post.frame_id = frame.id
                group by term_id, frame.field_id

                union all

                select term_id, field_id, -sum(frequency) as frequency, -count(distinct frame_id) as frames_occuring
                from old_unigram_posting post
                inner join disk_index.frame
                    on post.frame_id = frame.id
                group by term_id, field_id

                union all

                select term_id, field_id, frequency, frames_occuring
                from disk_index.term_statistics
                where term_id in (select left_id from phrase_map union select right_id from phrase_map)
            )
            insert or replace into disk_index.term_statistics(term_id, field_id, frequency, frames_occuring)
                select term_id, field_id, sum(frequency), sum(frames_occuring)
                from changed_terms
                group by term_id, field_id;


            -- Flush the updated data to the frame and term_posting
            insert or replace into disk_index.frame_posting(term_id, frame_id, frequency, positions)
                select *
                from update_term_posting;

            insert or replace into disk_index.term_posting(term_id, frame_id, frequency, positions)
                select *
                from update_term_posting;

            commit; detach disk_index; begin;
        """)

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
                (frame_count + self.frame_no, term, len(positions), json.dumps(positions))
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
        plugin_id = list(
            self._execute(
                "select plugin_id from plugin_registry where plugin_type = ? and settings = ?",
                (plugin_type, plugin_settings)
            )
        )

        if plugin_id is None:
            raise PluginNotFoundError('Plugin not found in this index')

        else:
            plugin_state = self._execute(
                "select key, value from plugin_data where plugin_id = ?;",
                plugin_id
            )

            for row in plugin_state:
                yield row

    def get_plugin_by_id(self, plugin_id):
        """Return the settings and state of the plugin identified by ID."""
        row = list(self._execute(
            'select plugin_type, settings from plugin_registry where plugin_id = ?', [plugin_id]
        ))
        if row is None:
            raise PluginNotFoundError
        plugin_type, settings = row
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
            """.format(where_clause),
            data
        )

        row = next(frames)

        if row is not None:
            current_term, frame_id, field_name, term_positions = row
            positions = {frame_id: json.loads(term_positions)}

            for term, frame_id, field_name, term_positions in frames:
                if term == current_term:
                    term_positions = json.loads(term_positions)
                    positions[frame_id] = term_positions
                else:
                    yield current_term, positions
                    positions = {frame_id: json.loads(term_positions)}
                    current_term = term
            else:
                yield current_term, positions

    def iterate_associations(self, term=None, include_fields=None, exclude_fields=None):
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

        Optionally a single term may be specified, in which case the associations for just that term will be
        returned.

        This method is a generator which yields a dict of {other_term: count, ...} for every term in the index.

        Note that this is dynamically calculated and may be expensive for large indexes.

        """

        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        # If no field selectivity is defined, we can leave off the two joins:
        if fields:
            joined_where = """
                inner join frame
                    on frame.id = post.frame_id
                inner join unstructured_field field
                    on field.id = frame.field_id
                {}""".format(where_clause)
        else:
            joined_where = ''

        term_filter = 'where outer.term = ?' if term is not None else ''
        terms = [term] if term is not None else []

        rows = self._execute(
            """
            with frames as (
                select vocab.term, frame_id
                from frame_posting post
                inner join vocabulary vocab
                    on post.term_id = vocab.id
                {}
            )
            select outer.term, inner.term, count(distinct outer.frame_id)
            from frames as outer
            inner join frames as inner
                on outer.frame_id = inner.frame_id
                and outer.term != inner.term
            {}
            group by outer.term, inner.term
            order by outer.term, inner.term""".format(joined_where, term_filter),
            fields + terms
        )

        first_row = rows.fetchone()
        if first_row is not None:
            current_term, other_term, count = first_row
            current_dict = {other_term: count}

            for term, other_term, count in rows:
                if current_term == term:
                    current_dict[other_term] = count
                else:
                    yield current_term, current_dict
                    current_term = term
                    current_dict = {other_term: count}
            # Make sure to yield the final row.
            yield current_term, current_dict
        else:
            yield None, {}

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
