# caterpillar: Tools for Sqlite backed data storage
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import os
import sqlite3

from caterpillar.data.storage import *


class SqliteStorage(Storage):
    """
    This class utilises Sqlite to store data structures to disk.

    Required Arguments:
    db_path -- full path of database file.

    Optional Arguments:
    acid -- determines whether database is ACID compliant (defaults to True).

    """

    CONTAINERS_TABLE = "containers"

    def __init__(self, db_path, acid=True):
        self.acid = acid
        self._db_path = db_path
        create = not os.path.exists(db_path)
        self._db = sqlite3.connect(db_path)

        if create:
            # Initialisation of new storage
            if acid is False:
                # Disable journaling
                self._db.execute("PRAGMA journal_mode = OFF")

            # Setup containers table
            self._db.execute(
                "CREATE TABLE {} (id VARCHAR PRIMARY KEY)".format(SqliteStorage.CONTAINERS_TABLE))

    @staticmethod
    def create(name, path, acid=True, containers=None):
        """
        Create an Sqlite storage object, stored on disk.

        Required Arguments:
        name -- name of database file to create.
        path -- path to store database under.

        Optional Arguments:
        acid -- determines whether database is ACID compliant (defaults to True).
        containers -- list of containers to initialise.

        """
        db_path = os.path.join(path, name)

        if os.path.isfile(db_path):
            raise DuplicateStorageError("Storage at path '{}' already exists".format(db_path))

        storage = SqliteStorage(db_path, acid)

        # Initialise containers
        if containers:
            for c_id in containers:
                storage.add_container(c_id)

        return storage

    @staticmethod
    def open(name, path):
        """
        Open an existing Sqlite storage object from disk.

        Required Arguments:
        name -- name of database file to open.
        path -- path database file is stored under.

        """
        db_path = os.path.join(path, name)

        if not os.path.exists(db_path):
            raise StorageNotFoundError("No storage at path '{}'".format(db_path))

        return SqliteStorage(db_path)

    def close(self):
        """
        Close the connection to the database for this storage object, rendering it UNUSABLE.

        """
        self._db.close()
        self._db = None

    def destroy(self):
        """
        Destroy this storage object, rendering it UNUSABLE.

        """
        self._db.close()
        os.remove(self._db_path)
        self._db = None

    def get_db_path(self):
        """
        Get the full path to the database file for this storage object.

        """
        return self._db_path

    def add_container(self, c_id):
        if self._has_container(c_id):
            raise DuplicateContainerError('\'{}\' container already exists'.format(c_id))
        cursor = self._db.cursor()
        cursor.execute("CREATE TABLE {} (key VARCHAR PRIMARY KEY, value TEXT NOT NULL)".format(c_id))
        cursor.execute("INSERT INTO {} VALUES (?)".format(SqliteStorage.CONTAINERS_TABLE), (c_id,))
        self._db.commit()

    def clear(self, container=None):
        if container:
            # Only clear the specified container
            self.clear_container(container)
        else:
            # Clear all containers
            cursor = self._db.cursor()
            for c_id in self._get_containers():
                cursor.execute("DROP TABLE {}".format(c_id))
            cursor.execute("DELETE FROM {}".format(self.CONTAINERS_TABLE))
            self._db.commit()

    def clear_container(self, c_id):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        self._db.execute("DELETE FROM {}".format(c_id))
        self._db.commit()

    def delete_container(self, c_id):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        cursor = self._db.cursor()
        cursor.execute("DROP TABLE {}".format(c_id))
        cursor.execute("DELETE FROM {} WHERE id = ?".format(self.CONTAINERS_TABLE), (c_id,))
        self._db.commit()

    def delete_container_item(self, c_id, key):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        self._db.execute("DELETE FROM {} WHERE key = ?".format(c_id), (key,))
        self._db.commit()

    def delete_container_items(self, c_id, keys):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        self._db.executemany("DELETE FROM {} WHERE key = ?".format(c_id), ((k,) for k in keys))
        self._db.commit()

    def get_container_len(self, c_id):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        cursor = self._db.cursor()
        cursor.execute("SELECT COUNT(*) FROM {}".format(c_id))
        return cursor.fetchone()[0]

    def get_container_keys(self, c_id):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        cursor = self._db.cursor()
        cursor.execute("SELECT key FROM {}".format(c_id))
        return [c[0] for c in cursor.fetchall()]

    def get_container_item(self, c_id, key):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        cursor = self._db.cursor()
        cursor.execute("SELECT value FROM {} WHERE key = ?".format(c_id), (key,))
        item = cursor.fetchone()
        if not item:
            raise KeyError("Key '{}' not found for container '{}'".format(key, c_id))
        return item[0]

    def get_container_items(self, c_id, keys=None):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        cursor = self._db.cursor()
        if keys:
            items = []
            for k in self._chunks(keys):
                cursor.execute("SELECT * FROM {} WHERE key IN ({})".format(c_id, ','.join(['?']*len(k))), k)
                items.extend(cursor.fetchall())
        else:
            cursor.execute("SELECT * FROM {}".format(c_id))
            items = cursor.fetchall()
        items = {k: v for k, v in items}
        # When a specific set of keys is provided, make sure they all exist in the returned dict
        if keys and len(keys) > len(items):
            for k in keys:
                k = unicode(k)
                if k not in items:
                    items[k] = None
        return items

    def set_container_item(self, c_id, key, value):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        self._db.execute("INSERT OR REPLACE INTO {} VALUES (?, ?)".format(c_id), (key, value))
        self._db.commit()

    def set_container_items(self, c_id, items):
        if not self._has_container(c_id):
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        self._db.executemany("INSERT OR REPLACE INTO {} VALUES (?,?)".format(c_id), (items.items()))
        self._db.commit()

    def _get_containers(self):
        """
        Return list of the containers in this storage.

        """
        cursor = self._db.cursor()
        cursor.execute("SELECT id FROM {}".format(SqliteStorage.CONTAINERS_TABLE))
        return [c[0] for c in cursor.fetchall()]

    def _has_container(self, c_id):
        """
        Return wheter the specified container exists on this storage.

        """
        cursor = self._db.cursor()
        cursor.execute("SELECT * FROM {} WHERE id = ?".format(SqliteStorage.CONTAINERS_TABLE), (c_id,))
        return cursor.fetchone() is not None

    @staticmethod
    def _chunks(l, n=999):
        """
        Yield successive n-sized chunks from l.

        """
        for i in xrange(0, len(l), n):
            yield l[i:i+n]


class SqliteMemoryStorage(SqliteStorage):
    """
    A version of SqliteStorage that uses an in-memory database.

    """

    @staticmethod
    def create(name, path=None, acid=True, containers=None):
        storage = SqliteMemoryStorage(':memory:', acid)

        # Initialise containers
        if containers:
            for c_id in containers:
                storage.add_container(c_id)

        return storage

    @staticmethod
    def open(name, path=None):
        raise NotImplementedError("Open method not supported for SqliteMemoryStorage")

    def destroy(self):
        """
        Destroy this storage object, rendering it UNUSABLE.

        """
        self.close()
