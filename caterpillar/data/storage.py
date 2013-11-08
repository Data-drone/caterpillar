# caterpillar: Tools for data storage
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import abc


class DuplicateContainerError(Exception):
    """A container by the same name already exists."""
    pass


class DuplicateStorageError(Exception):
    """A storage object already exists at the specified location."""
    pass


class ContainerNotFoundError(Exception):
    """No container by that name exists."""
    pass


class StorageNotFoundError(Exception):
    """No storage object found at the specified location."""


class Storage(object):
    """
    Abstract class used to store data on a disk.

    People who override this class need to provide a concrete implementation of various data storage methods. At a
    minimum a Storage object will need the schema for documents it is storing and in most cases where it is to be
    stored.

    BE AWARE: Although there is no way to enforce it via abc, all subclasses are expected to provide an open(path) and
    create(path, schema) class factory methods! The should open an existing storage location at path and create a new
    storage implementation at path with schema structure respectively.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def add_container(self, c_id):
        """
        Add a container with c_id to this Storage. Throws a ``DuplicateContainer`` error if there is already a container
        with this c_id.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.

        """
        return

    @abc.abstractmethod
    def delete_container(self, c_id):
        """
        Delete a container with c_id from this Storage. Throws a ``ContainerNotFound`` error if the c_id doesn't match a
        stored container.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.

        """
        return

    @abc.abstractmethod
    def get_container_item(self, c_id, key):
        """
        Retrieve a single item with key from container with c_id.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.
        key -- the string container key. This will be forced to a string by calling ``str`` on it.

        """
        return

    @abc.abstractmethod
    def get_container_items(self, c_id, keys=None):
        """
        Retrieve some or all the items stored in a container identified by c_id depending on the value of keys. If keys
        is none this method fetches all items otherwise this method fetches the items matching the keys in the keys
        list.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.

        Optional Arguments:
        keys -- the list of keys to fetch. Defaults to None meaning all items are fetched. All keys will be forced to
            strings by calling ``str`` on them.

        """
        return

    @abc.abstractmethod
    def set_container_item(self, c_id, key, value):
        """
        Add a single key value pair to this container.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.
        key -- the string item key. This will be forced to a string by calling ``str`` on it.
        value -- the string item value. This will be forced to a string by calling ``str`` on it.

        """
        return

    @abc.abstractmethod
    def set_container_items(self, c_id, items):
        """
        Add a numerous key value pairs to this container.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.
        items -- an iterable of string key value pairs to add. All keys and values of items  will be forced to
            strings by calling ``str`` on them.

        """
        return

    @abc.abstractmethod
    def delete_container_item(self, c_id, key):
        """
        Delete a single key value pair from this container.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.
        key -- the string item key. This will be forced to a string by calling ``str`` on it.

        """
        return

    @abc.abstractmethod
    def clear_container(self, c_id):
        """
        Clear all values from this container.

        Required Arguments:
        c_id -- the string container id. This will be forced to a string by calling ``str`` on it.

        """
        return

    @abc.abstractmethod
    def clear(self, container=None):
        """
        Clears this storage object of all data. If container is passed only clears container.

        """
        return

    @abc.abstractmethod
    def destroy(self):
        """
        Permanently destroy this storage object, deleting all data it encapsulates.

        BE WARNED, THIS IS IRREVERSIBLE!

        """
        return

    @abc.abstractmethod
    def create(name, path, acid=True, containers=None):
        pass

    @abc.abstractmethod
    def open(name, path):
        pass
