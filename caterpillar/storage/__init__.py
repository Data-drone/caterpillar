# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""
All communication with persistent storage devices is done via an implementation of :class:`.Storage`, which provides
a key/value object interface to the underlying persistence device and transactions so we can enforce ACID.

Anyone can write an implementation of :class:`.Storage`, but it is envisaged for the most part that these
implementations will usually be wrappers around other tools like SQLite or RockDB. No point re-inventing the wheel!

"""
import abc


class StorageError(Exception):
    """Base for all Storage exceptions."""


class DuplicateContainerError(StorageError):
    """A container by the same name already exists."""


class DuplicateStorageError(StorageError):
    """A storage object already exists at the specified location."""


class ContainerNotFoundError(StorageError):
    """No container by that name exists."""


class StorageNotFoundError(StorageError):
    """No storage object found at the specified location."""


class Storage(object):
    """
    Abstract class used to store key/value data on disk.

    Implementations of this class handle the storage of data to disk. The view of persistent storage presented here is
    one of key/value pairs stored in any number of containers.

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
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, path, create=False, readonly=False):
        """Open or create a storage instance."""
        return

    @abc.abstractmethod
    def begin(self):
        """Begin a transaction."""
        return

    @abc.abstractmethod
    def commit(self):
        """Commit a transaction"""
        return

    @abc.abstractmethod
    def rollback(self):
        """Rollback a transaction"""
        return

    @abc.abstractmethod
    def close(self):
        """Close this storage session and release all resources."""
        return

    @abc.abstractmethod
    def add_container(self, c_id):
        """
        Add a container with ``c_id`` (str) to this Storage.

        Throws a :exc:`DuplicateContainer` error if there is already a container with this ``c_id``.

        """
        return

    @abc.abstractmethod
    def delete_container(self, c_id):
        """
        Delete a container with ``c_id`` (str) from this Storage.

        Throws a :exc:`ContainerNotFound` error if the ``c_id`` doesn't match a stored container.

        """
        return

    @abc.abstractmethod
    def get_container_len(self, c_id):
        """Retrieve count of items in container with ``c_id`` (str)."""
        return

    @abc.abstractmethod
    def get_container_keys(self, c_id):
        """Generator across all keys from container with ``c_id`` (str)."""
        return

    @abc.abstractmethod
    def get_container_item(self, c_id, key):
        """Retrieve a single item with ``key`` from container with ``c_id``."""
        return

    @abc.abstractmethod
    def get_container_items(self, c_id, keys=None):
        """
        Generator across some or all the items stored in a container identified by ``c_id`` (str) depending on the value
        of ``keys`` (list).

        If ``keys`` is ``None`` this method fetches all items otherwise this method fetches the items matching the keys
        in the ``keys`` list.

        """
        return

    @abc.abstractmethod
    def set_container_item(self, c_id, key, value):
        """
        Add a single key/value pair to container ``c_id`` (str). Both ``key`` and ``value`` will be coerced to strings
        by calling :meth:`str`.

        """
        return

    @abc.abstractmethod
    def set_container_items(self, c_id, items):
        """
        Add numerous key value pairs to container ``c_id`` (str).

        ``items`` should be an iterable of string key/value pairs.

        """
        return

    @abc.abstractmethod
    def delete_container_item(self, c_id, key):
        """Delete the key/value pair from container ``c_id`` (str) identified by ``key`` (str)."""
        return

    @abc.abstractmethod
    def delete_container_items(self, c_id, keys):
        """Delete multiple key/value pairs from container ``c_id`` (str) using the iterable of ``keys``."""
        return

    @abc.abstractmethod
    def clear_container(self, c_id):
        """Clear all values from container ``c_id`` (str)."""
        return

    @abc.abstractmethod
    def clear(self):
        """Clears this storage object of all data."""
        return
