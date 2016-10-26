# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import abc


class AnalyticsPlugin(object):
    """

    - What is an analytics plugin?
    - What is it used for?
    - what are the limitations?
    - How do you use a plugin?
    - How do you implement a plugin?

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

    def __init__(self, index_reader, *args, **kwargs):
        """
        An instance of an ``AnalyticsPlugin`` takes an index and all settings necessary for it to operate.

        All settings (as distinct from plugin state which is derived from the specific index) should be included
        in the plugin initialisation.

        """
        self.index_reader = index_reader

    def load(self):
        """
        Convenience wrapper function to restore this plugin from the state stored in the index.

        """
        state = self.index_reader.get_plugin_state(self)
        self.restore_from_state(state)

    @abc.abstractmethod
    def run(self):
        """
        Run this instance of the plugin. This should perform all necessary calculations for the plugin to provide
        any functionality.

        Note that this run method can be called at any time after object instantiation - if necessary a

        """
        return

    @abc.abstractmethod
    def restore_from_state(self, state):
        """
        Restore this plugin instance to the state provided. The state is a dictionary of key-value pairs, as
        returned by the get_state method of the plugin.

        """
        return

    @abc.abstractmethod
    def get_name(self):
        """
        Get the name of this plugin. Used when storing the output of a plugin on an ``Index``.

        """
        return

    @abc.abstractmethod
    def get_settings(self):
        """
        Get a serialised representation of the settings for this plugin. This is used as a key for restoring
        plugin state, so should be consistent from instance to instance.

        This function must be callable after an instance of this plugin is created: the settings of a plugin
        must not be part of the state of any calculations performed during the running of this plugin.

        """
        return

    @abc.abstractmethod
    def get_state(self):
        """
        Get the state of this plugin. The returned state should be a dictionary of key-value pairs, with the
        keys and values already serialised appropriately for storage.

        """
        return
