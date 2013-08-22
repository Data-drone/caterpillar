# caterpillar: Plugable pieces of analytics.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import abc


class AnalyticsPlugin(object):
    """
    Plugins are registered on an index and allow external pieces of analytics to run on the index and store their
    results in a container.

    Plugins are run by the index and get passed an instance of the index with which to work. This allows them to access
    the underlying data structures of the index if they desire. They are expected to return a dict from
    string -> dict (string -> string) which will be stored on the index. Each item in the returned dict will be added as
    a container to the storage object of the index.

    Plugins must define a run() method by which they will be called. The method must return a dict of
    string -> dict(string -> string). They are also responsible for giving access to the underlying data structures they
    store on the index. How they do this is up to them.

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, index):
        """
        An instance of an ``AnalyticsPlugin`` always needs an index.

        Required Arguments:
        index -- an instance of ``Index``.

        """
        self._index = index

    @abc.abstractmethod
    def run(self, **fields):
        """
        The run method is how an index will call the plugin passing any arguments it was called with.

        This method must return a dict in the following format for storage on the index::

        {
            container_name: {
                key(str): value(str)
            },
            container_name: {
                key(str): value(str)
            }
        }

        """
        return

    @abc.abstractmethod
    def get_name(self):
        """
        Get the name of this plugin. Used when storing the output of a plugin on a ``Index``.

        """
        return