# caterpillar: Tools for data storage
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import abc
import copy


class DuplicateContainerError(Exception):
    """A container by the same name already exists."""
    pass


class ContainerNotFoundError(Exception):
    """No container by that name exists."""
    pass


class DocumentNotFoundError(Exception):
    """Can't find the specified documents."""
    pass


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
    def get_schema(self):
        """
        Return the ``Schema`` for this storage instance.

        """
        return

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
    def get_document(self, d_id):
        """
        Get a document using the passed d_id.

        Required Arguments:
        d_id -- the string id that identifies the document.

        Returns the document data as a dict.

        """
        return

    @abc.abstractmethod
    def store_document(self, d_id, data):
        """
        Store the passed document. It is the callers responsibility to update index structures.

        Required Arguments:
        d_id -- the string id that identifies the document.
        data -- a dict of document data to store which matches the schema for this index.

        """
        return

    @abc.abstractmethod
    def remove_document(self, d_id):
        """
        Remove a document from storage.

        Required Arguments:
        d_id -- the string id that identifies the document.

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
        Permanently destroy this index by removing all traces of it off any persistent storage it utilises.

        BE WARNED, THIS IS IRREVERSIBLE!

        """
        return


class RamStorage(Storage):
    """
    This class uses Python data structures to store relevant information in memory.

    This storage implementation doesn't define an open class method because it isn't persistent.

    BE WARNED: This class is not persistent! Things are ONLY stored in memory!

    """
    def __init__(self, schema, containers=None):
        if containers:
            self._containers = {str(c_id): {} for c_id in containers}
        else:
            self._containers = {}
        self._documents = {}
        self._schema = schema

    @classmethod
    def create(cls, schema, containers=None):
        return RamStorage(schema, containers)

    def destroy(self):
        self.clear()

    def clear(self, container=None):
        if container:
            self._containers[container].clear()
        else:
            self._documents.clear()
            self._containers.clear()

    def get_schema(self):
        return self._schema

    def store_document(self, d_id, data):
        self._documents[d_id] = data

    def remove_document(self, d_id):
        del self._documents[d_id]

    def get_document(self, d_id):
        try:
            return self._documents[d_id]
        except KeyError:
            raise DocumentNotFoundError('No such document {}'.format(d_id))

    def add_container(self, c_id):
        if str(c_id) in self._containers:
            raise DuplicateContainerError('\'{}\' container already exists'.format(c_id))
        self._containers[str(c_id)] = {}

    def delete_container(self, c_id):
        if str(c_id) not in self._containers:
            raise ContainerNotFoundError('No container \'{}\''.format(c_id))
        del self._containers[str(c_id)]

    def get_container_item(self, c_id, key):
        return self._containers[str(c_id)][str(key)]

    def get_container_items(self, c_id, keys=None):
        container = str(c_id)
        if keys:
            return {str(k): self._containers[container].get(str(k), '') for k in keys}
        else:
            return copy.deepcopy(self._containers[str(c_id)])

    def set_container_item(self, c_id, key, value):
        self._containers[str(c_id)][str(key)] = str(value)

    def set_container_items(self, c_id, items):
        self._containers[str(c_id)].update(items)

    def delete_container_item(self, c_id, key):
        del self._containers[str(c_id)][str(key)]

    def clear_container(self, c_id):
        self._containers[c_id].clear()