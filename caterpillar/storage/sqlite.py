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
import hashlib

import apsw

from caterpillar.storage import Storage, StorageNotFoundError, \
    DuplicateStorageError, PluginNotFoundError


logger = logging.getLogger(__name__)

# Three search cases:
# 1. Canonical representation
# 2. Canonical representation with some overrides
# 3. Complete vocabulary substitution.

# IndexReader.search.match_any(
#    terms, include_fields=None, exclude_fields=None,
#    lookup_variant=None, representation_variant=None
# )


def _hash_container_name(c_id):
    return hashlib.md5(c_id).hexdigest()


class SqliteStorage(Storage):
    """
    This class utilises SQLite to store data structures to disk.

    Reader / writer isolation here is provided by using `WAL mode <http://www.sqlite.org/wal.html>`_. There are no
    changes to the default checkpoint behaviour of SQLite, which at the time of writing defaults to 1000 pages.

    This storage type creates a new table for each container.

    """

    def __init__(self, path, create=False, readonly=False):
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
            self._db_connection = apsw.Connection(self._db, flags=apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE)
            cursor = self._db_connection.cursor()

            # Setup schema and necessary pragmas
            list(cursor.execute(_schema))

        elif readonly:
            self._db_connection = apsw.Connection(self._db, flags=apsw.SQLITE_OPEN_READONLY)
        else:
            self._db_connection = apsw.Connection(self._db, flags=apsw.SQLITE_OPEN_READWRITE)

        # We serialise writers during a write lock, and in normal cases the WAL mode avoids writers blocking
        # readers. Setting this is used to handle the one case in our normal operations that WAL mode requires
        # an exclusive lock for cleaning up the WAL file and associated shared-memory index.
        # See section 8 for the edge cases: https://www.sqlite.org/wal.html
        self._db_connection.setbusytimeout(1000)

    def begin(self, writer=False):
        """
        Begin a transaction.

        The write flag indicates if this is a transaction for a reader or a writer.

        For a writer a temporary in-memory database is created to cache results, which
        is destroyed after the commit or rollback methods are called.

        """

        if writer:
            # If we're opening for writing, don't connect to the index directly.
            # Instead setup a temporary in memory database with the minimal schema.
            self._cache = apsw.Connection(':memory:')
            list(self._cache.cursor().execute(_cache_schema))
            self._cache.cursor().execute('begin immediate')
        else:
            self._db_connection.cursor().execute('begin')

    def commit(self, writer=False):
        """Commit a transaction."""
        if writer:
            self._cache.cursor().execute(_flush_cache, [self._db])
            self._cache.cursor().execute('commit')
        else:
            self._db_connection.cursor().execute('commit')

    def rollback(self):
        """Rollback a transaction on an IndexWriter."""
        self._cache.cursor().execute('rollback')

    def close(self, writer=False):
        """Close this storage object and all its resources, rendering it UNUSABLE."""
        if writer:
            self._cache.close()
            self._cache = None

        self._db_connection.close()
        self._db_connection = None

    def add_structured_field(self, field_name):
        """Register a structured field on the index. """
        self._execute(self._cache, 'insert into structured_field(name) ?', [field_name])

    def add_unstructured_field(self, field_name):
        """Register an unstructured field on the index. """
        self._execute(self._cache, 'insert into unstructured_field(name) ?', [field_name])

    def delete_structured_field(self, field_name):
        """Delete a structured field and the associated data from the index."""
        return

    def delete_unstructured_field(self, field_name):
        """Delete an unstructured field from the index."""
        return

    def add_processed_document(self, document):
        """Add a processed (passed through an analyzer) document to the index. """
        return None

    def delete_document(self, document_id):
        """Delete a document with the given id from the index. """
        return None

    def add_container(self, c_id):
        """
        Add a data container identified by ``c_id`` (str).

        Raises :exc:`DuplicateContainerError` if there is already a container called ``c_id`` (str).

        """
        c_id = _hash_container_name(c_id)
        containers = self._get_containers()
        if c_id in containers:
            raise DuplicateContainerError('\'{}\' container already exists'.format(c_id))
        self._execute("CREATE TABLE \"{}\" (key VARCHAR PRIMARY KEY, value TEXT NOT NULL)".format(c_id))
        self._execute("INSERT INTO \"{}\" VALUES (?)".format(SqliteStorage.CONTAINERS_TABLE), (c_id,))

    def clear(self):
        """Clear all containers from storage."""
        for c_id in self._get_containers():
            self._execute("DROP TABLE \"{}\"".format(c_id))
        # Clear containers list
        self._execute("DELETE FROM \"{}\"".format(self.CONTAINERS_TABLE))

    def clear_container(self, c_id):
        """Clear all data in container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        self._execute("DELETE FROM \"{}\"".format(c_id))

    def delete_container(self, c_id):
        """Delete container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        self._execute("DROP TABLE \"{}\"".format(c_id))
        self._execute("DELETE FROM \"{}\" WHERE id = ?".format(self.CONTAINERS_TABLE), (c_id,))

    def delete_container_item(self, c_id, key):
        """Delete item ``key`` (str) from container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        self._execute("DELETE FROM \"{}\" WHERE key = ?".format(c_id), (key,))

    def delete_container_items(self, c_id, keys):
        """Delete items ``keys`` (str) from container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        self._executemany("DELETE FROM \"{}\" WHERE key = ?".format(c_id), ((k,) for k in keys))

    def get_container_len(self, c_id):
        """Get the number of rows in container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        cursor = self._execute("SELECT COUNT(*) FROM \"{}\"".format(c_id))
        return cursor.fetchone()[0]

    def get_container_keys(self, c_id):
        """Generator of keys from container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        cursor = self._execute("SELECT key FROM \"{}\"".format(c_id))
        while True:
            item = cursor.fetchone()
            if item is None:
                break
            yield item[0]

    def get_container_item(self, c_id, key):
        """Get item at ``key`` (str) from container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        cursor = self._execute("SELECT value FROM \"{}\" WHERE key = ?".format(c_id), (key,))
        item = cursor.fetchone()
        if not item:
            raise KeyError("Key '{}' not found for container '{}'".format(key, c_id))
        return item[0]

    def get_container_items(self, c_id, keys=None):
        """
        Generator of items at ``keys`` (list) in container ``c_id`` (str).

        If ``keys`` is None, iterates all items.

        """
        c_id = _hash_container_name(c_id)
        if keys is not None:  # If keys is none, return all keys, if keys is an empty set, return nothing
            keys = list(keys)
            for k in self._chunks(keys):
                cursor = self._execute("SELECT * FROM \"{}\" WHERE key IN ({})".format(c_id, ','.join(['?'] * len(k))), k)
                while True:
                    item = cursor.fetchone()
                    if item is None:
                        break
                    yield item
                    keys.remove(item[0])
            # Ensure we yield an item for every key
            for k in keys:
                yield (k, None,)
        else:
            cursor = self._execute("SELECT * FROM \"{}\"".format(c_id))
            while True:
                item = cursor.fetchone()
                if item is None:
                    break
                yield item

    def set_container_item(self, c_id, key, value):
        """Add ``key``/``value`` pair to container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        self._execute("INSERT OR REPLACE INTO \"{}\" VALUES (?, ?)".format(c_id), (key, value))

    def set_container_items(self, c_id, items):
        """Add the dict of key/value tuples to container ``c_id`` (str)."""
        c_id = _hash_container_name(c_id)
        self._executemany("INSERT OR REPLACE INTO \"{}\" VALUES (?,?)".format(c_id), (items.items()))

    def get_plugin_state(self, plugin_type, plugin_settings):
        """ """
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

    def set_plugin_state(self, plugin_type, plugin_settings, plugin_state):
        """ Set the plugin state in the index to the given state.

        Existing plugin state will be replaced.
        """
        # Check if there's an existing instance
        plugin_id = self._execute("select plugin_id from plugin_registry where plugin_type = ? and settings = ?",
                                  (plugin_type, plugin_settings)).fetchone()

        if plugin_id is not None:  # Clear all the data for this plugin instance
            self._execute("delete from plugin_data where plugin_id = ?; "
                          "delete from plugin_registry where plugin_id = ? ",
                          data=(plugin_id[0], plugin_id[0]))

        # Insert into the plugin registry. If plugin_id already existed, reuse it.
        plugin_id = self._execute(
            "insert into plugin_registry(plugin_type, settings, plugin_id) values (?, ?, ?); "
            "select last_insert_rowid();",
            (plugin_type, plugin_settings, plugin_id[0] if plugin_id is not None else None)
        ).fetchone()[0]
        insert_rows = ((plugin_id, key, value) for key, value in plugin_state.iteritems())
        self._executemany("insert into plugin_data values (?, ?, ?);", insert_rows)
        return plugin_id

    def delete_plugin_state(self, plugin_type, plugin_settings=None):
        """"""
        if plugin_settings is not None:
            self._execute(
                "delete from plugin_data "
                "where plugin_id in (select plugin_id from plugin_registry where plugin_type = ? and settings = ?);",
                [plugin_type, plugin_settings]
            )
            self._execute(
                "delete from plugin_registry where plugin_type = ? and settings = ?;", [plugin_type, plugin_settings]
            )
        else:
            self._execute(
                "delete from plugin_data "
                "where plugin_id in (select plugin_id from plugin_registry where plugin_type = ?);",
                [plugin_type]
            )
            self._execute("delete from plugin_registry where plugin_type = ?;", [plugin_type])

    def list_known_plugins(self):
        """ Return a list of (plugin_type, settings, id) triples for each plugin stored in the index. """
        return [row for row in self._execute("select plugin_type, settings, plugin_id from plugin_registry;")
                if row is not None]

    def _execute(self, conn, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.execute(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise e

    def _executemany(self, conn, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.executemany(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise e

    @staticmethod
    def _chunks(l, n=999):
        """Yield successive n-sized chunks from l."""
        l = list(l)
        for i in xrange(0, len(l), n):
            yield l[i:i+n]


_schema = """
pragma journal_mode = WAL;
pragma page_size = 4096;

begin;

/* Field names and ID's

Structured and unstructured fields are kept separate because the storage and querying
of each type of data is different.
*/
create table structured_field (
    id integer primary key,
    name text unique
);

create table unstructured_field (
    id integer primary key,
    name text unique
);

/*
The core vocabulary table assigns an integer ID to every unique term in the index.

Joins against the core posting tables are always integer-integer and in sorted order.
*/
create table vocabulary (
    id integer primary key,
    token text unique
);


/* The source table for the document representation. */
create table document (
    id integer primary key,
    stored text -- This should be a text serialised representation of the document, such as JSON.
);

/*
Storage for 'indexed' structured fields in the schema.

- designed for sparse data and extensible schema's
- primary design purpose is for returning lists of document ID's
- takes advantage of SQLite's permissive type system

*/
create table document_data (
    document_id integer,
    field_id integer,
    value,
    primary key(field_id, value, document_id),
    foreign key(document_id) references document(id),
    foreign key(field_id) references structured_field(id)
);



create table frame (
    id integer primary key,
    document_id integer,
    field_id integer,
    stored text -- The stored representation of the frame.
);


/* Index to access by document ID

Bridges between:
structured data searches --> frames
unstructured searches --> documents
*/
create index document_frame_bridge on frame(document_id, field_id);


/* Postings organised by term, allowing search operations. */
create table term_posting (
    term_id integer,
    frame_id integer,
    frequency integer,
    primary key(term_id, frame_id),
    foreign key(term_id) references term(id),
    foreign key(frame_id) references frame(id)
)
without rowid; -- Ensures that the data in the base table is kept in this sorted order.

/* Postings organised by frame, for term-frequency vector representations of documents and frames. */
create table frame_posting (
    frame_id integer,
    term_id integer,
    frequency integer,
    primary key(frame_id, term_id),
    foreign key(term_id) references term(id) on delete cascade
    foreign key(frame_id) references frame(id) on delete cascade
)
without rowid;


/* Plugin header and data tables. */
create table plugin_registry (
    plugin_type text,
    settings text,
    plugin_id integer primary key,
    constraint unique_plugin unique (plugin_type, settings) on conflict replace
);

create table plugin_data (
    plugin_id integer,
    key text,
    value text,
    primary key(plugin_id, key) on conflict replace,
    foreign key(plugin_id) references plugin_registry(plugin_id) on delete cascade
);


commit;

"""

"""/* A whitelist of vocabulary variant columns in the vocabulary table.

When a variation is first registered a column is created in the table for that name.
*/
create table vocabulary_variant(
    id integer primary key,
    name text
);

/* Document_id's for soft deletion.

Wherever possible the document content is deleted, but it is not always possible to do so
efficiently.
*/
create table deleted_document (
    document_id integer primary key
);

/* Summary statistics for a given term_id by field.

This allows direct lookups for Tf.IDF searches and similar.
*/
create table term_statistics (
    term_id integer,
    field_id integer,
    frequency integer,
    frames_occuring integer,
    documents_occuring integer,
    primary key(term_id, field_id) on conflict replace,
    foreign key(term_id) references term(id),
    foreign key(field_id) references field(id),
);
/*
An internal representation of the state of the index documents.

Each count is incremented by one when a document is added or deleted. Both numbers are
monotonically increasing and the system is serialised: these numbers can be used to represent
the current state of the system, and can be used to measure some degree of change between
different versions.

For example, if a plugin was run at revision (100, 4), and the current state of the index is
(200, 50), then there is a significant difference between the corpus at the time the plugin
was run and now.

*/
create table index_revision (
    added_document_count integer,
    deleted_document_count integer
)

/*
A convenience view to simplify writing search queries.

If we move to a segmented or otherwise optimised index structure this view will
combine the tables, so queries should use this in preference to direct table references.

Note that this view uses the canonical representation of the term to represent variants.
*/

create view search_posting as (
    select *
    from term_posting
    inner join vocabulary
        on term_posting.term_id = vocabulary.id
    inner join frame
        on term_posting.frame_id = frame.id
    inner join field
        on frame.field_id = field.id
    where document_id not in (select document_id from deleted_documents)
);
"""

# Schema for the staging database
_cache_schema = """
begin;

create table structured_field (
    name text primary key
);
create table unstructured_field (
    name text primary key
);

/* The source table for the document representation. */
create table document (
    id integer primary key,
    stored text -- This should be a text serialised representation of the document, such as JSON.
);

/* Storage for 'indexed' structured fields in the schema. */
create table document_data (
    document_id integer,
    field_name text,
    value,
    primary key(document_id, field_name)
);

create table frame (
    id integer primary key,
    document_id integer,
    field_name text,
    stored text -- The stored representation of the frame.
);

/* One row per occurence of a term in a frame */
create table positions_staging (
    frame_id integer,
    document_id integer,
    field_name text,
    term_name text
);

commit;
"""

# Flush changes from the cache to the index
_flush_cache = """
-- Generate final views into all of the temporary data structures
    -- Term-document_id ordering
    -- Term statistics summary
    -- Generate necessary indexes
-- Generate statistics of deleted documents for removal

commit; -- end surrounding transaction so we can attach on disk database.

-- Attach the on disk database to flush to.
attach database ? as disk_index;

begin; -- Begin the true transaction for on disk writing

insert into disk_index.structured_field(name) select * from structured_field;
insert into disk_index.unstructured_field(name) select * from unstructured_field;

-- Delete the documents
    -- Delete documents
    -- delete frames
    -- Add a tombstone for that document_id
-- Update the vocabulary
-- Insert new documents
    -- Insert document
    -- Insert frames
    -- Insert postings
-- Update the statistics
-- Update the plugins

"""
