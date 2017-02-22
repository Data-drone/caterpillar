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
from __future__ import division
import logging
import math
import os

import apsw

from caterpillar.storage import StorageWriter, StorageReader, Storage, StorageNotFoundError, \
    DuplicateStorageError, PluginNotFoundError

from ._sqlite_schema import disk_schema, cache_schema, prepare_flush, flush_cache


logger = logging.getLogger(__name__)


class SqliteWriter(StorageWriter):
    """
    This class uses SQLite to write data structures to disk.

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

            # Setup schema and necessary pragmas (on disk). Note that the disk_schema script returns rows from the
            # pragma statements - the list call makes sure that all statements are run, even if we don't care about the
            # returned rows.
            list(cursor.execute(disk_schema))

            # The database is never written to directly, hence it is closed after initialisation.
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

        return self.__last_added_documents, self.__deleted_documents, self.__updated_plugins

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
        self.__updated_plugins = list(
            self._execute(
                flush_cache,
                {
                    'max_doc': max_document_id + 1,
                    'max_frame': max_frame_id + 1,
                    'deleted': deleted_count + len(self.__deleted_documents),
                    'added': self.doc_no + max_document_id,
                    'added_frames': self.frame_no + max_frame_id
                })
        )
        self._flushed = True  # Only needed for _merge_term_variants currently.

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
                A string representing the format of the passed data. Currently only 'v1'
                is supported.
            document_data:
                The data for the document, in the format expected for document_data.

        Valid Document Formats

            document_format == 'v1':
            An iterable of
                - a string representation of the whole document
                - a dictionary of field_name:field_value pairs for the document level structured data
                - a dictionary {
                    field_name: list of string representations of each frames
                }
                - a dictionary {
                    field_name: list of {term: [[word1 token_position], [word2 token_position]]} vectors for each frame
                }
            For the frame data (3rd and 4th elements), the frames should be in document sequence order
            and there should be a one-one correspondence between frame representations and term:frequency vectors.

        """
        if document_format == 'v1':
            try:
                # Create a savepoint so we don't have any problems with the field addition.
                self._execute('savepoint document')

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
                number_frame_terms = {field: len(values) for field, values in frame_terms.iteritems()}

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
                frame_term_data = (
                    frame for field, frame_list in sorted(frame_terms.iteritems()) for frame in frame_list
                )
                insert_term_data = (
                    # The leading and trailing [] are stripped so positions can be concatenated as strings.
                    (frame_count + self.frame_no, term, len(positions), _bitwise_encode(positions))
                    for frame_count, frame_data in enumerate(frame_term_data)
                    for term, positions in frame_data.iteritems()
                )

                self._executemany(
                    'insert into stage_posting(frame_id, term, frequency, positions) values (?, ?, ?, ?)',
                    insert_term_data
                )

                self._execute('release document')  # rollup this savepoint into the transaction.
                self.frame_no += total_frames
                self.doc_no += 1

            except Exception as e:
                self._execute('rollback to savepoint document')
                raise e
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
    Reader class for data stored in SQLite by SQLiteWriter.

    A reader is transactionally isolated from writers by SQLite's Write Ahead Log. Calling the begin() method
    of this class begins a read transaction that does not end until commit is explicitly called.

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
        self._execute('begin')
        # Temporary table for searches - only visible to this reader.
        self._execute("""
            create temporary table term_search_driver(
                term_id integer,  -- Drives the lookup in the index
                --These three columns drive the counts for deciding on matches
                all_id integer,
                n_id integer,
                exclude_count integer,
                -- Weighting for that particular term
                weight float default 1.0
            )
            """)

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

    def iterate_term_frequencies(self, terms=None, include_fields=None, exclude_fields=None):
        """Return a generator of frequencies over the list of terms supplied. """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        if terms is not None:
            term_filter = 'and voc.term in ({})'.format(', '.join(['?'] * len(terms)))
        else:
            term_filter = ''

        terms = terms or []

        frequencies = self._execute("""
            select voc.term, sum(frames_occuring)
            from term_statistics stats
            inner join vocabulary voc
               on voc.id = stats.term_id
               {}
            inner join unstructured_field field
               on stats.field_id = field.id
               {}
            group by voc.term
            """.format(term_filter, where_clause), terms + fields
        )
        return frequencies

    def _iterate_positions(self, terms=None, include_fields=None, exclude_fields=None):
        """Iterate through the positions index, giving frame ids and frequencies for matching terms.

        By default, all terms are iterated. Optionally a list of terms can be provided.

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
            select vocab.term, frame.id, field.name, post.frequency
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

        for term, frame_id, field_name, frequency in frames:
            if current_term is None:
                positions = {frame_id: frequency}
                current_term = term
            elif term == current_term:
                positions[frame_id] = frequency
            else:
                yield current_term, positions
                positions = {frame_id: frequency}
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

    def iterate_term_frequency_vectors(self, weighting='tf', include_fields=None, exclude_fields=None, frame_ids=None):
        """
        Iterates through sparse term_vectors for frames in the index.

        Currently only term frequency 'tf' weighting is supported.

        If frame_ids is provided, then the include_fields and exclude_fields arguments will be ignored.

        """

        if frame_ids is None:
            where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

            field_join = """
                inner join frame
                    on frame.id = frame_posting.frame_id
                inner join unstructured_field field
                    on field.id = frame.field_id
            """ if fields else ''
            rows = self._execute("""
                select frame_id, term, frequency
                from frame_posting
                inner join vocabulary
                    on frame_posting.term_id = vocabulary.id
                {}
                {}
                order by frame_id
            """.format(field_join, where_clause), fields)
        else:
            rows = self._executemany("""
                select frame_id, term, frequency
                from frame_posting
                inner join vocabulary
                    on frame_posting.term_id = vocabulary.id
                where frame_id = ?
            """, ((i,) for i in frame_ids))

        current_frame = None

        for frame_id, term, frequency in rows:
            if current_frame is None:
                term_freqs = {term: frequency}
                current_frame = frame_id

            elif current_frame == frame_id:
                term_freqs[term] = frequency

            else:
                yield current_frame, term_freqs
                current_frame = frame_id
                term_freqs = {term: frequency}

        else:  # Make sure to yield the final row.
            if current_frame is not None:
                yield current_frame, term_freqs

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

    def iterate_bigram_positions(self, bigrams, include_fields=None, exclude_fields=None):
        """Return an iterator of (left_term, right_term, frame_id, frequency) tuples for the specified list of bigrams.

        Bigrams are supplied as a list of tuples: [('apple', 'pie'), ('whipped', 'cream')].

        Currently, only exact matches for each term are considered - if one of the terms in the bigram occurs
        after the 63rd position in a frame it is not considered a match.

        """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        if fields:
            extra_join = """
                inner join frame
                    on frame.id = right_post.frame_id
                inner join unstructured_field field
                    on field.id = frame.field_id
            """
            # Mangle the normal where clause for fielded search: this query is difficult to structure to match this.
            extra_where = 'and ' + where_clause[5:]
        else:
            extra_where = extra_join = ''

        query_data = (list(bigram) + fields for bigram in bigrams)
        bigrams = self._executemany("""
            select
                left_vocab.term,
                right_vocab.term,
                left_post.frame_id,
                left_post.positions & (right_post.positions >> 1) as matched_positions
            from term_posting left_post
            inner join term_posting right_post
                on left_post.frame_id = right_post.frame_id
            inner join vocabulary left_vocab
                on left_vocab.id = left_post.term_id
                and left_vocab.term = ?
            inner join vocabulary right_vocab
                on right_vocab.id = right_post.term_id
                and right_vocab.term = ?
            {}
            where
                -- Exclude approximate positions indexes
                left_post.positions > 0
                and right_post.positions > 0
                -- And they actually have matching positions
                and matched_positions > 0
                {}
        """.format(extra_join, extra_where), query_data
        )

        for left_term, right_term, frame_id, positions in bigrams:
            yield ((left_term, right_term), frame_id, _count_bitwise_matches(positions))

    def filter_metadata(
        self, metadata, return_documents=False, include_fields=None, exclude_fields=None,
        limit=0, pagination_key=None
    ):
        """
        Support metadata only searches - for efficiency reasons search_or_filter_unstructured
        is driven by term_posting table. This function provides efficient set filtering for
        metadata only cases.

        Currently only conjunctive metadata queries are supported, with the exception of the 'in'
        operator for matching multiple alternatives.

        The include_field and exclude_field arguments refer to unstructured fields only - these
        options are ignored if return_documents is True.

        """

        metadata_clauses = []
        parameters = []
        # Whitelist of valid operators - we can't bind an operator in sql, so reject everything else.
        valid_metadata_operators = set(('<', '>', '<=', '>=', '=', 'in'))

        for metadata_field, operators in metadata.items():

            # Template for each field - fill in the field name and append the parameters.
            this_field = """
                select document_id from document_data
                where field_id = (select id from structured_field where name = ?)
            """

            if return_documents and pagination_key:
                this_field += 'and document_id > ?'
                parameters.append(pagination_key)

            parameters.append(metadata_field)

            for operator, value in operators.items():
                if operator not in valid_metadata_operators:
                    raise ValueError('{} is not a supported operator'.format(operator))

                if operator == 'in':  # The values for an 'in' operator should be an iterable.
                    this_field += 'and value {} ({}) \n'.format(operator, ', '.join(['?'] * len(value)))
                    parameters.extend(value)

                else:
                    this_field += 'and value {} ? \n'.format(operator)
                    parameters.append(value)

            metadata_clauses.append(this_field)

        # Note that all metadata queries are conjunctions only: all metadata clauses must be matched.
        document_intersection = '\n intersect \n'.join(metadata_clauses)

        if return_documents:
            if limit:
                document_intersection += 'order by document_id \n limit ?'
                parameters.append(limit)
            results = self._execute(document_intersection, parameters)

        else:
            frame_intersection = """
                select id
                from frame
                where document_id in (
                    {}
                )
                {}
                {}
                {}
                """
            where_field, fields = self._fielded_where_clause(include_fields, exclude_fields)
            if fields:
                field_selector = 'and field_id in (select field.id from unstructured_field field {})'
                field_selector = field_selector.format(where_field)
                parameters.extend(fields)
            else:
                field_selector = ''
            if pagination_key:
                pagination_clause = 'and frame_id > ?'
                parameters.append(pagination_key)
            else:
                pagination_clause = ''
            if limit:
                limit_clause = 'order by frame_id \n limit ?'
                parameters.append(limit)
            else:
                limit_clause = ''

            results = self._execute(
                frame_intersection.format(document_intersection, field_selector, pagination_clause, limit_clause),
                parameters
            )

        return results

    def rank_or_filter_unstructured(
        self, return_documents=False, include_fields=None, exclude_fields=None,
        must=None, should=None, at_least_n=None, must_not=None, metadata=None,
        limit=0, pagination_key=None, search=False
    ):
        """
        Omnibus function for searching and filtering unstructured data.

        Because most of the logic is similar across filtering and searching, this function
        supports everything. The IndexReader provides the coherent interface to present
        to the user.

        Currently only conjunctive metadata searches are supported.

        The cost of a search is driven by the cost of the largest intermediate term set:
        a search in a large English language index for the term 'and' will be expensive,
        regardless of which criteria (must, should etc.) is used as all rows containing that
        term will need to aggregated over.

        Scoring is currently based on TF-IDF. Frames are scored individually. Document scores are
        computed by summing frame scores. The inverse 'document' frequency is actually the
        inverse frame frequency of that term across the included unstructured fields in this
        query.

        For filtering, the pagination key is the last frame_id or document_id seen, for search,
        the pagination key is a tuple of (frame_id, score). Pagination based on a deterministic
        key gives better performance on deep paging queries (later queries are slightly cheaper
        rather than more and more expensive), and also gives a more stable sort with respect to
        relevance when new results occur.

        Data model / search conceptualisation

        Consider the index as a giant table:

        term | frame | document | field | metadata_field | meta_data_field | metadata_field

        Specification of term variants:
            Specifying a single search term is done by providing the string representation of the
            term. Alternatively, you can specify a sequence of alternative terms. For example:
            must=[('dog', 'canine'), 'food'] will return documents or frames containing the word
            'food' and at least one of 'dog' or 'canine'.

        """

        # This is the query we will be passing through to SQLite. The remainder of this function
        # just fills in the gaps, conditioned on all the options specified by the user.
        query = """
            select
                {frame_or_document},
                /* Scoring Note

                The scores are quantized to tf-idf * 10 as an integer for two reasons:
                    1. Pagination depends on exact score comparison - floats are not reliable for
                       this, especially when considering network transfers, encoding in JSON etc.
                    2. Robustness to small changes in IDF values - for example if a pagination
                       request occurs after some new documents are added to the result set.

                */
                cast(sum(frequency * ts.weight)*10 as integer) as score
            from term_search_driver ts

            /* Optimisation Note

            Note that in SQLite cross join behaves identically to inner join, *except* the query
            optimiser does not reorder the joins. The join order is critical for this case - if
            term_posting is reordered we may end up with a full table scan of a very large table.

            */

            cross join term_posting post
                on ts.term_id = post.term_id

            {subset_clause}

            {filter_pagination}

            {groupby}

            having 1 -- no-op clause to simplify query construction.
                {having}

            {search_clause}
        """

        # Parameter handling, expand optional parameters for the rest of the function
        must = must or []
        should = should or []
        at_least_n = at_least_n or (0, [])
        must_not = must_not or []
        metadata = metadata or {}

        # Raise an error if must_not is specified without should, must or at_least_n
        if must_not and not(at_least_n[1] or should or must):
            raise ValueError(
                '"must_not" is not supported for SQLiteStorage without at least one term in '
                '"must", "should" or "at_least_n"'
            )

        # Expand singular terms into lists for each variant to support variant search.
        # ['term1', 'term2', ('term3', 'term4')] --> [('term1'), ('term2'), ('term3', 'term4')]
        # Each tuple of terms is assigned a single search_id for determining matches.
        # Note that for must_not and should the variation syntax is supported, even if there
        # is no difference.
        search_term_groups = [_unpack_mixed_term_list(group) for group in [must, at_least_n[1], must_not, should]]
        search_terms = [[term] for group in search_term_groups for terms in group for term in terms]

        # If there are no terms or metadata specified, there are no results as this search
        # is driven by the matching terms in must, should and at_least_n.
        if len(search_terms) == 0 and not metadata:
            return [None]

        # Construct one row per term in the search to insert in the driving table.
        # Note that grouped terms get assigned the same search_id - this is magic to make it work.
        search_id_rows = []
        search_id = 1

        for i, group in enumerate(search_term_groups):
            for terms in group:
                for term in terms:
                    # Columns: all_id, n_id, exclude_count, (not included in table)
                    search_row = [None, None, 0, None]
                    search_row[i] = search_id
                    search_id_rows.append(search_row)
                search_id += 1

        # Generate the where clause, including the metadata specific details.
        unstructured_where_clause, unstructured_fields = self._fielded_where_clause(include_fields, exclude_fields)

        # Compute IDF component of the term weighting from the term_statistics on this index
        n_frames = list(self._execute(
            'select sum(frame_count) '
            'from field_statistics '
            'inner join unstructured_field field on field_statistics.field_id = field.id ' +
            unstructured_where_clause, unstructured_fields)
        )[0][0]

        term_stats = list(self._executemany(
            """
            select sum(frames_occuring) as frame_frequency
            from term_statistics
            inner join vocabulary
                on term_statistics.term_id = vocabulary.id
            where vocabulary.term = ?
            """, search_terms)
        )

        # Early exit if none of the terms match.
        if len(term_stats) == 1 and term_stats[0][0] is None:
            if search:
                return []
            else:
                return {}

        term_idf = [1 + math.log(n_frames / (n[0] + 1)) for n in term_stats]

        # Truncate the temporary driving table
        # Note that because of how searches work, this means that only a single search query
        # can be active for a given reader, as SQLite temporary tables are not isolated across
        # queries. For this reason, although this method can potentially be used as a generator,
        # the IndexReader API always returns the complete resultset.
        self._execute('delete from term_search_driver')

        # Stage the terms to the driving table, including the necessary weighting
        self._executemany("""
            insert into term_search_driver(term_id, all_id, n_id, exclude_count, weight)
                select id as term_id, ?2, ?3, ?4, ?5
                from vocabulary
                where term = ?1
                order by term_id
            """, [term + row[:3] + [weight] for term, row, weight in zip(search_terms, search_id_rows, term_idf)]
        )

        parameters = []

        if metadata or unstructured_fields or return_documents:
            subset_clause = 'inner join frame on post.frame_id = frame.id '

            if unstructured_fields:
                subset_clause += ' and frame.field_id in (select id from unstructured_field field ' \
                    + unstructured_where_clause + ')'
                parameters += unstructured_fields

            # Note that by this point, all of the metadata values must be analysed and the operators validated by the
            # IndexReader. In this storage layer we are dealing only with the representation of the value in the
            # database.
            if metadata:

                metadata_clauses = []
                # Whitelist of valid operators - we can't bind an operator in sql, so reject everything else.
                valid_metadata_operators = set(('<', '>', '<=', '>=', '=', 'in'))

                for metadata_field, operators in metadata.items():

                    this_field = (
                        'select document_id from document_data '
                        'where field_id = (select id from structured_field where name = ?)'
                    )
                    parameters.append(metadata_field)

                    for operator, value in operators.items():
                        if operator not in valid_metadata_operators:
                            raise ValueError('{} is not a supported operator'.format(operator))

                        if operator == 'in':  # The values for an 'in' operator should be an iterable.
                            this_field += 'and value {} ({}) \n'.format(operator, ', '.join(['?'] * len(value)))
                            parameters.extend(value)

                        else:
                            this_field += 'and value {} ? \n'.format(operator)
                            parameters.append(value)

                    metadata_clauses.append(this_field)

                # Note that all metadata queries are intersections: all metadata clauses must be matched.
                subset_clause += 'and document_id in ({})'.format(' intersect '.join(metadata_clauses))

        else:
            subset_clause = ''

        if not search and return_documents and pagination_key is not None:
            filter_pagination = 'where document_id > ?'
            parameters.append(pagination_key)
        elif not search and pagination_key is not None:
            filter_pagination = 'where frame_id > ?'
            parameters.append(pagination_key)
        else:
            filter_pagination = ''

        if return_documents:
            groupby_clause = 'group by document_id'
        else:
            groupby_clause = 'group by frame_id'

        having_clause = ''
        # Construct term_inclusion clauses: avoid checking things that aren't needed.
        if must:
            # if all_count is NULL, term || all_count evaluates to null, and is not counted in the distinct
            having_clause += 'and count(distinct all_id) = ? '
            parameters.append(len(must))

        if at_least_n[0]:
            having_clause += 'and count(distinct n_id) >= ? '
            parameters.append(at_least_n[0])

        if must_not:
            having_clause += 'and max(exclude_count) = 0 '

        search_clause = ''

        if search:
            if pagination_key is not None:
                having_clause += 'and (score < ? or (score = ? and {} > ?))'.format(
                    'document_id' if return_documents else 'frame_id'
                )
                parameters.extend([pagination_key[1], pagination_key[1], pagination_key[0]])

            # If we're searching, order by score descending and frame/document_id ascending for deterministic pagination
            search_clause += 'order by score desc, {} '.format('document_id' if return_documents else 'frame_id')

        elif limit:  # If we are paging through filter result sets, make sure we order by document_id or frame_id
            search_clause += 'order by {} '.format('document_id' if return_documents else 'frame_id ')

        if limit:
            search_clause += 'limit ?'
            parameters.append(limit)

        # Now fill in the template, and actually execute the query.
        results = self._execute(
            query.format(
                frame_or_document='document_id' if return_documents else 'frame_id',
                filter_pagination=filter_pagination,
                subset_clause=subset_clause,
                groupby=groupby_clause,
                having=having_clause,
                search_clause=search_clause
            ), parameters
        )

        return results

    def find_significant_bigrams(self, include_fields=None, exclude_fields=None, min_count=5, threshold=40):
        """Find significant collocations of words.

        Currently operates over all fields in the index.

        Currently, only exact matches for each term are considered - if one of the terms in the bigram occurs
        after the 63rd position in a frame it is not considered a match.

        Algorithm Notes

        The formula for calculating bi-gram score is inspired by the Gensim implementation of phrase detection from the
        Mikolov et al paper, "Distributed Representations of Words and Phrases and their Compositionality".

        score(a, b) = freq(a, b) * vocab_size / (freq(a) * freq(b))

        Currently the frequencies are the number of frames a bigram/unigram occurs in.

        Args

            min_count: specifies the minimum number of times a bigram must occur to be considered. It is also
                used to prefilter the vocabulary for terms that don't occur enough to form a bigram.
            threshold: the value of the statistical threshold used to determine if a phrase is a match or not.

        """
        where_clause, fields = self._fielded_where_clause(include_fields, exclude_fields)

        # If fields are specified, we have some extra work to do.
        if fields:
            post_join = """
                inner join frame
                    on frame.id = right_post.frame_id
                inner join unstructured_field field
                    on field.id = frame.field_id
            """
            # Mangle the normal where clause for fielded search: this query is difficult to structure to match this.
            post_where = 'and ' + where_clause[5:]
            term_join = 'inner join unstructured_field field on field.id = ts.field_id and ' + where_clause[5:]
        else:
            post_where = post_join = term_join = ''

        bigrams = self._execute("""
            with bigrams as (
                select
                    left_post.term_id as left_id,
                    right_post.term_id as right_id,
                    count(*) * 1.0 as bigram_count
                from frame_posting left_post
                inner join frame_posting right_post
                    on left_post.frame_id = right_post.frame_id
                {}
                where
                    -- Exclude approximate positions indexes
                    left_post.positions > 0
                    and right_post.positions > 0
                    -- And they actually have matching positions
                    and (left_post.positions & (right_post.positions >> 1)) > 0
                    {}
                group by left_post.term_id, right_post.term_id
                having bigram_count > ?
            ),
            field_statistics as (
                select ts.term_id, term, sum(frames_occuring) as frames_occuring
                from term_statistics ts
                inner join vocabulary
                    on vocabulary.id = ts.term_id
                {}
                group by ts.term_id, term
            )
            select left_stats.term, right_stats.term
            from bigrams
            inner join field_statistics left_stats
                on left_stats.term_id = bigrams.left_id
            inner join field_statistics right_stats
                on right_stats.term_id = bigrams.right_id
            where (
                bigram_count * (select count(*) from field_statistics) /
                (1.0 * left_stats.frames_occuring * right_stats.frames_occuring)
            ) > ?
            """.format(post_join, post_where, term_join),
            fields + [min_count] + fields + [threshold]
        )

        return bigrams

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
        # Catch None as a valid field to allow current reader level interface to specify None as a field.
        invalid_fields = [field for field in fields if field not in valid_fields and field is not None]

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


def _bitwise_encode(ordinal_positions):
    """
    Converts the sorted list of integers to a bitstring.

    The integer i is indicated in the positions by setting the i'th bit of the string to 1.

    Superpositions of integers up to 62 (positions 0, 1, ... 62) can be represented exactly.

    For integers larger than 62, an approximate matching scheme is used: the position i % 63
    is recorded instead. If the match is approximate, the high bit will set: the output integer is
    negative if the match is approximate.

    """

    p = 0

    for i in ordinal_positions:
        p |= 1 << i % 63

    if i > 62:
        p = -p

    return p


def _count_bitwise_matches(position_bitstring):
    """ Count the number of matches (number of bits set to 1) indicated by the given bitstring.

    If the high bit is set in the bitstring, this will return 0.

    This uses Kernighan's algorithm, and is faster in the case of a small number of bits set.

    """
    if position_bitstring <= 0:
        return 0

    n = 0

    while position_bitstring > 0:
        position_bitstring &= position_bitstring - 1
        n += 1

    return n


def _unpack_mixed_term_list(term_sequence):
    """ Unpack the list of terms into term groups.

    Assume that anything this isn't a string is an iterable that can be left along.

    """
    return [
        [group] if isinstance(group, basestring) else group for group in term_sequence
    ]
