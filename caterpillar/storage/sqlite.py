# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""
An sqlite implementation of :class:`caterpillar.storage.Storage`.

The only class is :class:`.SqliteStorage` which uses sqlite in WAL mode to achieve reader/writer isolation.

"""
import logging
import os

import apsw

from caterpillar.storage import Storage, StorageNotFoundError, DuplicateContainerError, ContainerNotFoundError, \
    DuplicateStorageError, PluginNotFoundError


logger = logging.getLogger(__name__)


_plugin_table = """
begin;
create table plugin_registry (
    name text,
    settings text,
    plugin_id integer primary key,
    constraint unique_plugin unique (name, settings) on conflict replace);

create table plugin_data (
    plugin_id integer,
    key text,
    value text,
    primary key(plugin_id, key) on conflict replace,
    foreign key(plugin_id) references plugin_registry(plugin_id) on delete cascade);
commit; """


class SqliteStorage(Storage):
    """
    This class utilises SQLite to store data structures to disk.

    Reader / writer isolation here is provided by using `WAL mode <http://www.sqlite.org/wal.html>`_. There are no
    changes to the default checkpoint behaviour of SQLite, which at the time of writing defaults to 1000 pages.

    This storage type creates a new table for each container.

    """

    CONTAINERS_TABLE = "containers"

    def __init__(self, path, create=False, readonly=False):
        """
        Initialise a new instance of this storage at ``path`` (str).

        If ``create`` (bool) is False and path doesn't exist then a :exc:`StorageNotFoundError` is raised. Otherwise,
        if ``create`` is True then we create the database if it doesn't already exist.

        If ``create`` is True and the DB already exist, then :exc:`DuplicateStorageError` is raised.

        """
        self._db_path = path
        db = os.path.join(path, 'storage.db')

        if not create and not os.path.exists(db):
            raise StorageNotFoundError('Can\'t find the resources required by SQLiteStorage. Is it corrupt?')
        elif create and os.path.exists(db):
            raise DuplicateStorageError('There already appears to be something stored at {}'.format(path))

        if create:
            self._db_connection = apsw.Connection(db, flags=apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE)
            cursor = self._db_connection.cursor()
            # Enable WAL
            cursor.execute("PRAGMA journal_mode = WAL")

            # Setup containers table
            cursor.execute("BEGIN; CREATE TABLE {} (id VARCHAR PRIMARY KEY); COMMIT;"
                           .format(SqliteStorage.CONTAINERS_TABLE))

            # Setup plugin data tables
            cursor.execute(_plugin_table)

        elif readonly:
            self._db_connection = apsw.Connection(db, flags=apsw.SQLITE_OPEN_READONLY)
        else:
            self._db_connection = apsw.Connection(db, flags=apsw.SQLITE_OPEN_READWRITE)

    def begin(self):
        """Begin a transaction."""
        self._db_connection.cursor().execute('BEGIN')

    def commit(self):
        """Commit a transaction."""
        self._db_connection.cursor().execute('COMMIT')

    def rollback(self):
        """Rollback a transaction."""
        self._db_connection.cursor().execute('ROLLBACK')

    def close(self):
        """Close this storage object and all its resources, rendering it UNUSABLE."""
        self._db_connection.close()
        self._db_connection = None

    def add_container(self, c_id):
        """
        Add a data container identified by ``c_id`` (str).

        Raises :exc:`DuplicateContainerError` if there is already a container called ``c_id`` (str).

        """
        containers = self._get_containers()
        if c_id in containers:
            raise DuplicateContainerError('\'{}\' container already exists'.format(c_id))
        self._execute("CREATE TABLE {} (key VARCHAR PRIMARY KEY, value TEXT NOT NULL)".format(c_id))
        self._execute("INSERT INTO {} VALUES (?)".format(SqliteStorage.CONTAINERS_TABLE), (c_id,))

    def clear(self):
        """Clear all containers from storage."""
        for c_id in self._get_containers():
            self._execute("DROP TABLE {}".format(c_id))
        # Clear containers list
        self._execute("DELETE FROM {}".format(self.CONTAINERS_TABLE))

    def clear_container(self, c_id):
        """Clear all data in container ``c_id`` (str)."""
        self._execute("DELETE FROM {}".format(c_id))

    def delete_container(self, c_id):
        """Delete container ``c_id`` (str)."""
        self._execute("DROP TABLE {}".format(c_id))
        self._execute("DELETE FROM {} WHERE id = ?".format(self.CONTAINERS_TABLE), (c_id,))

    def delete_container_item(self, c_id, key):
        """Delete item ``key`` (str) from container ``c_id`` (str)."""
        self._execute("DELETE FROM {} WHERE key = ?".format(c_id), (key,))

    def delete_container_items(self, c_id, keys):
        """Delete items ``keys`` (str) from container ``c_id`` (str)."""
        self._executemany("DELETE FROM {} WHERE key = ?".format(c_id), ((k,) for k in keys))

    def get_container_len(self, c_id):
        """Get the number of rows in container ``c_id`` (str)."""
        cursor = self._execute("SELECT COUNT(*) FROM {}".format(c_id))
        return cursor.fetchone()[0]

    def get_container_keys(self, c_id):
        """Generator of keys from container ``c_id`` (str)."""
        cursor = self._execute("SELECT key FROM {}".format(c_id))
        while True:
            item = cursor.fetchone()
            if item is None:
                break
            yield item[0]

    def get_container_item(self, c_id, key):
        """Get item at ``key`` (str) from container ``c_id`` (str)."""
        cursor = self._execute("SELECT value FROM {} WHERE key = ?".format(c_id), (key,))
        item = cursor.fetchone()
        if not item:
            raise KeyError("Key '{}' not found for container '{}'".format(key, c_id))
        return item[0]

    def get_container_items(self, c_id, keys=None):
        """
        Generator of items at ``keys`` (list) in container ``c_id`` (str).

        If ``keys`` is None, iterates all items.

        """
        if keys is not None:  # If keys is none, return all keys, if keys is an empty set, return nothing
            keys = list(keys)
            for k in self._chunks(keys):
                cursor = self._execute("SELECT * FROM {} WHERE key IN ({})".format(c_id, ','.join(['?'] * len(k))), k)
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
            cursor = self._execute("SELECT * FROM {}".format(c_id))
            while True:
                item = cursor.fetchone()
                if item is None:
                    break
                yield item

    def set_container_item(self, c_id, key, value):
        """Add ``key``/``value`` pair to container ``c_id`` (str)."""
        self._execute("INSERT OR REPLACE INTO {} VALUES (?, ?)".format(c_id), (key, value))

    def set_container_items(self, c_id, items):
        """Add the dict of key/value tuples to container ``c_id`` (str)."""
        self._executemany("INSERT OR REPLACE INTO {} VALUES (?,?)".format(c_id), (items.items()))

    def get_plugin_state(self, plugin_name, plugin_settings):
        """ """
        plugin_id = self._execute("select plugin_id from plugin_registry where name = ? and settings = ?",
                                  (plugin_name, plugin_settings)).fetchone()

        if plugin_id is None:
            raise PluginNotFoundError('Plugin not found in this index')

        else:
            plugin_state = self._execute("select key, value from plugin_data where plugin_id = ?;",
                                         plugin_id)
            for row in plugin_state:
                yield row

    def set_plugin_state(self, plugin_name, plugin_settings, plugin_state):
        """ Set the plugin state in the index to the given state.

        Existing plugin state will be replaced.
        """
        # Insert into the plugin registry
        plugin_id = self._execute(
            "insert into plugin_registry(name, settings) values (?, ?);"
            "select last_insert_rowid();",
            (plugin_name, plugin_settings)
        ).fetchone()[0]
        insert_rows = ((plugin_id, key, value) for key, value in plugin_state.iteritems())
        self._executemany("insert into plugin_data values (?, ?, ?);", insert_rows)

    def delete_plugin_state(self, plugin_name, plugin_settings=None):
        """"""
        if plugin_settings is not None:
            self._execute(
                "pragma foreign_keys=ON;"
                "delete from plugin_registry where name = ? and settings = ?;"
                "pragma foreign_keys=OFF;",
                [plugin_name, plugin_settings]
            )
        else:
            self._execute("pragma foreign_keys=ON; "
                          "delete from plugin_registry where name = ?;"
                          "pragma foreign_keys=OFF;",
                          [plugin_name])

    def list_known_plugins(self):
        """ Return a list of (name, settings) pairs for each plugin stored in the index. """
        return [row for row in self._execute("select name, settings from plugin_registry;") if row is not None]

    def _get_containers(self):
        """Return list of all containers regardless of storage type."""
        cursor = self._db_connection.cursor()
        cursor.execute("SELECT id FROM {}".format(SqliteStorage.CONTAINERS_TABLE))
        return [c[0] for c in cursor.fetchall()]

    def _execute(self, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.execute(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise ContainerNotFoundError("No such container")

    def _executemany(self, query, data=None):
        cursor = self._db_connection.cursor()
        try:
            return cursor.executemany(query, data)
        except apsw.SQLError as e:
            logger.exception(e)
            raise ContainerNotFoundError("No such container")

    @staticmethod
    def _chunks(l, n=999):
        """Yield successive n-sized chunks from l."""
        l = list(l)
        for i in xrange(0, len(l), n):
            yield l[i:i+n]
