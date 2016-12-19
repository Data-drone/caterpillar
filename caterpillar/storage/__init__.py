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


class StorageWriter(object):
    """
    Abstract class used to modify the index.

    Implementations of this class handle the writing of content to disk.

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
    def __init__(self, path, create=False):
        """Open or create a storage instance."""
        return

    @abc.abstractmethod
    def begin(self):
        """Begin a transaction to modify the index."""
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
    def add_structured_fields(self, field_names):
        """Register a structured field on the index. """
        return

    @abc.abstractmethod
    def add_unstructured_fields(self, field_names):
        """Register an unstructured field on the index. """
        return

    @abc.abstractmethod
    def delete_structured_fields(self, field_names):
        """Delete a structured field and the associated data from the index."""
        return

    @abc.abstractmethod
    def delete_unstructured_fields(self, field_names):
        """Delete an unstructured field from the index."""
        return

    @abc.abstractmethod
    def add_analyzed_document(self, document, structured_data, frames):
        """Take a document analyzed by an analyzer, and store it.

        TODO: specify the datatype expected here.
        TODO: work out how we can do this backwards compatibly? ie, specify a type string?

        """
        return

    @abc.abstractmethod
    def delete_documents(self, document_ids):
        """Delete the given documents."""
        return

    @abc.abstractmethod
    def set_plugin_state(self, plugin_name, plugin_settings, plugin_state):
        """Save the state of the given plugin"""
        return

    @abc.abstractmethod
    def delete_plugin_state(self, plugin_name, plugin_settings=None):
        """Delete all plugin data for ``plugin_name``, or optionally only the data for the ``plugin_settings`` instance.

         """
        return


class StorageReader(object):
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

    Access methods that can be specified to access specific fields should always implement this using the
    following optional keyword arguments:
        include_fields = ['field1', 'field2', 'field3']
        exclude_fields = ['field4']
    If both include fields and exclude fields are specified then the former should take priority and the
    latter ignored. Wherever practical the return values should treat the fields included in the query as a
    single logical field by concatenating or aggregating as appropriate.


    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, path):
        """Open or create a reader for the given storage location."""
        return

    @abc.abstractmethod
    def begin(self):
        """Begin a read transaction."""
        return

    @abc.abstractmethod
    def commit(self):
        """End the read transaction."""
        return

    @abc.abstractmethod
    def close(self):
        """Close this storage session and release all resources."""
        return

    @abc.abstractmethod
    def list_known_plugins(self):
        """Return a list of (plugin_name, plugin_settings, plugin_id) stored in this index."""
        return

    @abc.abstractproperty
    def structured_fields(self):
        """Get a list of the structured field names on this index."""
        return

    @abc.abstractproperty
    def unstructured_fields(self):
        """Get a list of the unstructured field names on this index."""
        return

    @abc.abstractproperty
    def revision(self):
        """An object representing the current revision number of this index."""
        return

    @abc.abstractmethod
    def get_plugin_state(self, plugin_name, plugin_settings):
        """Return a dictionary of key-value pairs identifying that state of this plugin."""
        return

    @abc.abstractmethod
    def get_plugin_by_id(self, plugin_id):
        """Return the settings and state of the plugin identified by ID."""
        return
