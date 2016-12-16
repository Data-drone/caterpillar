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


class DuplicateStorageError(StorageError):
    """A storage object already exists at the specified location."""


class StorageNotFoundError(StorageError):
    """No storage object found at the specified location."""


class PluginNotFoundError(StorageError):
    """No data for found for this plugin."""


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
    def begin(self, writer=False):
        """Begin a transaction."""
        return

    @abc.abstractmethod
    def commit(self, writer=False):
        """Commit a transaction"""
        return

    @abc.abstractmethod
    def rollback(self):
        """Rollback a transaction"""
        return

    @abc.abstractmethod
    def close(self, writer=False):
        """Close this storage session and release all resources."""
        return

    @abc.abstractmethod
    def add_processed_document(self, document):
        """Take a document analyzed by an analyzer, and store it. """
        return

    @abc.abstractmethod
    def delete_document(self, document_id):
        """Delete the given document."""
        return

    @abc.abstractmethod
    def set_plugin_state(self, plugin_name, plugin_settings, plugin_state):
        """Save the state of the given plugin"""
        return

    @abc.abstractmethod
    def get_plugin_state(self, plugin_name, plugin_settings):
        """Return a dictionary of key-value pairs identifying that state of this plugin."""
        return

    @abc.abstractmethod
    def get_plugin_by_id(self, plugin_id):
        """Return the settings and state of the plugin identified by ID."""
        return

    @abc.abstractmethod
    def delete_plugin_state(self, plugin_name, plugin_settings=None):
        """Delete all plugin data for ``plugin_name``, or optionally only the data for the ``plugin_settings`` instance.

         """
        return

    @abc.abstractmethod
    def list_known_plugins(self):
        """Return a list of (plugin_name, plugin_settings, plugin_id) stored in this index."""
        return
