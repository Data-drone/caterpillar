# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import abc


class AnalyticsPlugin(object):
    """
    An ``AnalyticsPlugin`` provides a mechanism to provide additional functionality on top of the operations already
    defined by an ``IndexReader``. Plugins also allow for saving and restoring plugin state within the index.

    Usage::

        # Instantiate a plugin object
        plugin = ExamplePlugin(reader, alpha=2)

        # Run the plugin over the provided ``IndexReader``
        plugin.run()

        # Store the results in the index with an ``IndexWriter``
        writer.set_plugin_state(plugin)

        # Later on restore the state of the plugin
        plugin = ExamplePlugin(reader, alpha=2)
        plugin.load()

        # Use plugin functionality as provided by the methods and attributes of the plugin instance
        results = plugin.useful_function(argument1, argument2)
        x = plugin.precalculated_statistic

    Implementation:

        Plugins need to provide four things:

        1. An __init__ method that takes at least an ``IndexReader`` as an argument.
        2. A serialised representation of the plugin's name and settings, to uniquely identify an instance of a plugin.
        3. A run method to act as a consistent entrypoint. The plugin should be ready after calling this method.
        4. A serialised representation of the plugin's state, and the means to restore from that representation.

        These are provided by providing implementations for the abstract methds of this ``AnalyticsPlugin`` class.

    Limitations:

        - Plugins are designed for calculations that fit in memory and storage of small amounts of data. The plugin
          data store is not especially optimised for cases where large amounts of data need to be stored.
        - Plugins are responsible for serialising and deserialising their own state.
        - Care needs to taken with serialising the settings of a plugin, as the serialised representation is used as
          a key in the plugin registry to identify unique plugin instances. This means that, for example, Python
          dictionaries should not be directly used as the order of keys is not guaranteed.

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

        Note that this run method can be called at any time after object instantiation - since storing a plugin
        is separate from running the plugin on an index the run method can work incrementally.

        """
        return

    @abc.abstractmethod
    def restore_from_state(self, state):
        """
        Restore this plugin instance to the state provided. The state is a dictionary of key-value pairs, as
        returned by the get_state method of the plugin.

        The plugin instance is responsible for appropriately serialising and deserialising a representation
        of its own state.

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
