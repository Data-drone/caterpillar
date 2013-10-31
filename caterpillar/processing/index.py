# caterpillar: Tools to create and store a text index
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@mammothlabs.com.au>
from __future__ import division
import ujson as json
import logging
import uuid

import nltk
from caterpillar.data.storage import RamStorage, DuplicateContainerError
from caterpillar.processing.analysis.analyse import PotentialBiGramAnalyser
from caterpillar.processing.analysis.tokenize import ParagraphTokenizer
from caterpillar.searching import IndexSearcher
from caterpillar.searching.scoring import TfidfScorer


logger = logging.getLogger(__name__)


class Index(object):
    """
    An index of all the text within a data set; holds statistical information to be used in retrieval and analytics.

    Required Arguments:
    schema -- a ``schema.Schema`` instance that describes the structure of documents that reside in this index.
    storage -- a ``data.storage.Storage`` instance that will be used to write and read this index into storage.

    """

    POSITIONS_CONTAINER = "positions"
    ASSOCIATIONS_CONTAINER = "associations"
    FREQUENCIES_CONTAINER = "frequencies"
    FRAMES_CONTAINER = "frames"

    def __init__(self, schema, storage):
        self._schema = schema
        self._storage = storage
        self._plugins = dict()

    @staticmethod
    def create(schema, storage_cls=RamStorage, **args):
        """
        Create a new index object with the specified path, schema and a instance of the passed storage class.

        Required Arguments:
        schema -- the ``Schema`` for the index.
        storage_cls -- the class to instantiate for the storage object. Defaults to ``RamStorage``. The create()
                       method will be called on this instance.
        **args -- any keyword arguments that need to be passed onto the storage instance.
        """
        storage = storage_cls.create(schema, [
            Index.POSITIONS_CONTAINER,
            Index.ASSOCIATIONS_CONTAINER,
            Index.FREQUENCIES_CONTAINER,
            Index.FRAMES_CONTAINER
        ], **args)
        return Index(schema, storage)

    @staticmethod
    def open(path, storage_cls):
        """
        Open an existing Index with the given path.

        Required Arguments:
        path -- the string path to the stored index.
        storage_cls -- the class to instantiate for the storage object. Defaults to ``RamStorage``. The open() method
                       will be called on this instance.

        """
        #storage = storage_cls.open(path)
        #return Index(storage.get_schema(), storage)
        raise NotImplementedError  # TODO: Only makes sense when we have a persistent storage backed

    def destroy(self):
        """
        Permanently destroy this index.

        """
        self._storage.destroy()

    def get_positions_index(self):
        """
        Returns a dict of term positions for this Index.

        This is what is known as an inverted text index. Structure is as follows::

        {
            term: {
                frame_id: [(start, end), (start, end)],
                ...
            },
            ...
        }

        """
        return {k: json.loads(v) for k, v in self._storage.get_container_items(Index.POSITIONS_CONTAINER).items()}

    def get_term_positions(self, term):
        """
        Returns a dict of term positions for term.

        This is what is known as an inverted text index. Structure is as follows::

        {
            frame_id1: [(start, end), (start, end)],
            frame_id2: [(start, end), (start, end)],
            ...
        }

        """
        return json.loads(self._storage.get_container_item(Index.POSITIONS_CONTAINER, term))

    def get_associations_index(self):
        """
        Returns a dict of term associations for this Index.

        This is used to record when two terms co-occur in a document. Be aware that only 1 co-occurrence for two terms
        is recorded per document not matter the frequency of each term. The format is as follows::

        {
            term: {
                other_term: count,
                ...
            },
            ...
        }

        """
        return {k: json.loads(v) for k, v in self._storage.get_container_items(Index.ASSOCIATIONS_CONTAINER).items()}

    def get_term_association(self, term, assc):
        """
        Returns a count of term associations between term and assc.

        Required Arguments:
        term -- the str term.
        assc -- the str associated term.

        """
        return json.loads(self._storage.get_container_item(Index.ASSOCIATIONS_CONTAINER, term))[assc]

    def get_frequencies(self):
        """
        Returns a dict of term frequencies for this Index.

        Be aware that a terms frequency is only incremented by 1 per document not matter the frequency within that
        document. The format is as follows::

        {
            term: count
        }

        """
        return {k: json.loads(v) for k, v in self._storage.get_container_items(Index.FREQUENCIES_CONTAINER).items()}

    def get_term_frequency(self, term):
        """
        Return the frequency of term as an int.

        Required Arguments:
        term -- the str term.

        """
        return json.loads(self._storage.get_container_item(Index.FREQUENCIES_CONTAINER, term))

    def get_frame_count(self):
        """
        Return the count of frames stored on this index.

        """
        return len(self._storage.get_container_items(Index.FRAMES_CONTAINER))

    def get_frame(self, frame_id):
        """
        Fetch a frame by frame_id.

        Required Arguments:
        frame_id -- the str id of the frame.

        """
        return json.loads(self._storage.get_container_item(Index.FRAMES_CONTAINER, frame_id))

    def get_document(self, d_id):
        """
        Returns the document with the given d_id as a dict.

        Required Arguments:
        d_id -- the string id of the document.

        """
        return self._storage.get_document(d_id)

    def searcher(self, scorer_cls=TfidfScorer):
        """
        Return a searcher for this Index.

        """
        return IndexSearcher(self, scorer_cls)

    def add_document(self, frame_size=2, fold_case=False, update_index=True, encoding='utf-8', **fields):
        """
        Add a document to this index.

        We index text breaking it into frames for analysis. The frame_size param controls the size of those frames.
        Setting frame_size to a number < 1 will result in all text being put into one frame or, to put it another way,
        the text not being broken up into frame.

        The fields need to match the schema for this Index otherwise an InvalidDocumentStructure exception will be
        thrown.

        If you are adding a large number of documents in one hit (calling this method repeatedly) then you should
        consider setting update_index to False for those calls and instead calling ``reindex()`` after all those calls.
        This will deliver a significant speed boost to the index process.

        Required Arguments:
        **fields -- the fields and their values for this document. Calling this method will look something like this:
                    writer.add_document(field1=value1, field2=value2).

        Option Arguments:
        frame_size -- the int size of the text frames to break each indexed ``TEXT`` field of this document into.
                      Defaults to 2. A value < 1 will result in only frame being created containing all the text.
        fold_case -- A boolean flag indicating whether to perform case folding after re-indexing.
        update_index -- A boolean flag indicating whether to update the index after adding this document or not.
        encoding -- this str argument is passed to ``str.decode()`` to decode all text fields.

        """
        logger.info('Adding document')
        schema_fields = self._schema.items()
        document_id = uuid.uuid4().hex
        sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

        # STEP 1 - Build index structures.
        positions = {}  # Inverted term positions index:: term -> [(start, end,), (star,end,), ...]
        associations = {}  # Inverted term co-occurrence index:: term -> other_term -> count
        frequencies = nltk.probability.FreqDist()  # Inverted term frequency index:: term -> count
        frames = {}  # Frame data:: frame_id -> {key: value}

        # First we need to build the shell of all frames. The shell includes all non-indexed fields
        shell_frame = {}
        for field in schema_fields:
            if not field[1].indexed() and field[0] in fields:
                shell_frame[field[0]] = fields[field[0]]

        # Tokenize fields that need it
        logger.info('Starting tokenization')
        frame_count = 0
        for field in schema_fields:
            if field[1].indexed() and field[0] in fields:  # Only index field if it's in the schema and is to be indexed
                # Break up into paragraphs first
                paragraphs = ParagraphTokenizer().tokenize(fields[field[0]].decode(encoding))
                for paragraph in paragraphs:
                    # Next we need the sentences grouped by frame
                    sentences = sentence_tokenizer.tokenize(paragraph.value, realign_boundaries=True)
                    sentences_by_frames = [sentences[i:i+frame_size] for i in xrange(0, len(sentences), frame_size)]
                    for sentence_list in sentences_by_frames:
                        # Build our frames
                        frame_id = "{}-{}".format(document_id, frame_count)
                        frame_count += 1
                        frame = {
                            '_id': frame_id,
                            '_text': ". ".join(sentence_list),
                            '_field': field[0],
                            '_positions': {},
                            '_associations': {},
                            '_sequence_number': frame_count,
                            '_doc_id': document_id,
                        }
                        for sentence in sentence_list:
                            # Tokenize and index
                            tokens = field[1].analyse(sentence)

                            # Record positional information
                            for token in tokens:
                                # Add to the list of terms we have seen if it isn't already there.
                                if not token.stopped:
                                    # Record word positions on index structure and on frame
                                    try:
                                        positions[token.value][frame_id].append(token.index)
                                    except KeyError:
                                        try:
                                            positions[token.value][frame_id] = [token.index]
                                        except KeyError:
                                            positions[token.value] = {frame_id: [token.index]}
                                            associations[token.value] = {}
                                    try:
                                        frame['_positions'][token.value].append(token.index)
                                    except KeyError:
                                        frame['_positions'][token.value] = [token.index]

                        # Record co-occurrences and frequencies for the terms we have seen in the frame
                        for term in frame['_positions'].keys():
                            # Record frequency information on index
                            frequencies.inc(term)
                            for other_term in frame['_positions'].keys():
                                if term != other_term:
                                    # Record word associations on text index and frame
                                    associations[term][other_term] = associations[term].get(other_term, 0) + 1
                                    try:
                                        frame['_associations'][term].add(other_term)
                                    except KeyError:
                                        frame['_associations'][term] = set()
                                        frame['_associations'][term].add(other_term)
                        # Build the final frame
                        frame.update(shell_frame)
                        frames[frame_id] = frame
        logger.info('Tokenization complete. {} frames created.'.format(len(frames)))

        # STEP 2 - Store the frames we have created and optionally update the index data structures.
        if update_index:
            logger.info('Updating index.')
            # Start by fetching what we need
            positions_index = {k: json.loads(v) if v else {} for k, v in self._storage.get_container_items(
                Index.POSITIONS_CONTAINER, positions.keys()).items()}
            assocs_index = {k: json.loads(v) if v else {} for k, v in self._storage.get_container_items(
                Index.ASSOCIATIONS_CONTAINER, associations.keys()).items()}
            frequencies_index = {k: json.loads(v) if v else 0 for k, v in self._storage.get_container_items(
                Index.FREQUENCIES_CONTAINER, frequencies.keys()).items()}

            # Now update the indexes
            # Positions First
            for term, indices in positions.items():
                for frame_id, index in indices.items():
                    try:
                        positions_index[term][frame_id] = positions_index[term][frame_id] + index
                    except KeyError:
                        positions_index[term][frame_id] = index
            for key, value in positions_index.items():
                positions_index[key] = json.dumps(positions_index[key])
            self._storage.set_container_items(Index.POSITIONS_CONTAINER, positions_index)

            # Associations next
            for term, value in associations.items():
                for other_term, count in value.items():
                    assocs_index[term][other_term] = assocs_index[term].get(other_term, 0) + count
            for key, value in assocs_index.items():
                assocs_index[key] = json.dumps(assocs_index[key])
            self._storage.set_container_items(Index.ASSOCIATIONS_CONTAINER, assocs_index)

            # Finally frequencies
            for key, value in frequencies.items():
                frequencies_index[key] = frequencies_index[key] + value
            for key, value in frequencies_index.items():
                frequencies_index[key] = json.dumps(frequencies_index[key])
            self._storage.set_container_items(Index.FREQUENCIES_CONTAINER, frequencies_index)

        # Store the frames - really easy
        self._storage.set_container_items(Index.FRAMES_CONTAINER, {k: json.dumps(v) for k, v in frames.items()})

        # Finally add the document to storage.
        fields.update({'_id': document_id})
        self._storage.store_document(document_id, fields)

        if fold_case:
            logger.info('Performing case folding.')
            self.fold_term_case()

        return document_id

    def reindex(self, fold_case=False):
        """
        Re-build in the index based on the tokenization information stored on the frames. These frames were previously
        generated by adding documents to this index.

        Please note, re-indexing DOES NOT re-run tokenization on the frames. Tokenization is only performed once on a
        document and the output from that tokenization stored against the frame.

        Consider using this method when adding a lot of documents to the index at once. In that case, each call to
        ``add_document()`` would set update_index to False when calling that method and instead call this method after
        adding all the documents. This will deliver a significant speed boost to the indexing process in that situation.

        Optional Arguments:
        fold_case -- A boolean flag indicating whether to perform case folding after re-indexing.

        """
        logger.info('Re-building the index.')
        positions = {}  # Inverted term positions index:: term -> [(start, end,), (star,end,), ...]
        associations = {}  # Inverted term co-occurrence index:: term -> other_term -> count
        frequencies = nltk.probability.FreqDist()  # Inverted term frequency index:: term -> count
        frames = self._storage.get_container_items(Index.FRAMES_CONTAINER)  # All frames on the index

        for data in frames.values():
            frame = json.loads(data)
            frame_id = frame['_id']

            # Positions & frequencies First
            for term, indices in frame['_positions'].items():
                frequencies.inc(term)
                try:
                    positions[term][frame_id] = positions[term][frame_id] + indices
                except KeyError:
                    try:
                        positions[term][frame_id] = indices
                    except KeyError:
                        positions[term] = {frame_id: indices}
            # Associations next
            for term, other_terms in frame['_associations'].items():
                for other_term in other_terms:
                    try:
                        associations[term][other_term] = associations[term].get(other_term, 0) + 1
                    except KeyError:
                        associations[term] = {other_term: 1}

        # Now serialise and store the various parts
        # Positions
        for key, value in positions.items():
            positions[key] = json.dumps(positions[key])
        self._storage.set_container_items(Index.POSITIONS_CONTAINER, positions)
        # Associations
        for key, value in associations.items():
            associations[key] = json.dumps(associations[key])
        self._storage.set_container_items(Index.ASSOCIATIONS_CONTAINER, associations)
        # Frequencies
        frequencies_index = {}
        for key, value in frequencies.items():
            frequencies_index[key] = json.dumps(frequencies[key])
        self._storage.set_container_items(Index.FREQUENCIES_CONTAINER, frequencies_index)
        logger.info("Re-indexed {} frames.".format(len(frames)))

        if fold_case:
            logger.info('Performing case folding.')
            self.fold_term_case()

    def delete_document(self, d_id):
        """
        Delete the document with given id.

        Raises a DocumentNotFound exception if the id doesn't match any document.

        Required Arguments:
        id -- the string id of the document to delete.

        """
        self._storage.remove_document(d_id)

    def fold_term_case(self, merge_threshold=0.7):
        """
        Perform case folding on the index, merging words into names and vice-versa depending on the specified threshold.

        Optional Arguments:
        merge_threshold -- A float used to test when to merge two variants. When the ratio between word and name version
                           of term falls below this threshold the merge is carried out.

        """
        count = 0
        frequencies_index = {k: json.loads(v) if v else 0
                             for k, v in self._storage.get_container_items(Index.FREQUENCIES_CONTAINER).items()}
        # Because getting the associations index is so expensive we just do it once and pass it round.
        associations_index = {k: json.loads(v) if v else {}
                              for k, v in self._storage.get_container_items(Index.ASSOCIATIONS_CONTAINER).items()}
        for w, freq in frequencies_index.items():
            if w.islower() and w.title() in frequencies_index:
                freq_name = frequencies_index[w.title()]
                if freq / freq_name < merge_threshold:
                    # Merge into name
                    logger.debug('Merging {} & {}'.format(w, w.title()))
                    self._merge_terms(w, w.title(), associations_index)
                    count += 1
                elif freq_name / freq < merge_threshold:
                    # Merge into word
                    logger.debug('Merging {} & {}'.format(w.title(), w))
                    self._merge_terms(w.title(), w, associations_index)
                    count += 1
        self._storage.clear(Index.ASSOCIATIONS_CONTAINER)
        self._storage.set_container_items(Index.ASSOCIATIONS_CONTAINER,
                                          {k: json.dumps(v) for k, v in associations_index.items()})
        logger.info("Merged {} terms during case folding.".format(count))

    def _merge_terms(self, old_term, new_term, associations):
        storage = self._storage
        old_positions = json.loads(storage.get_container_item(Index.POSITIONS_CONTAINER, old_term))
        new_positions = json.loads(storage.get_container_item(Index.POSITIONS_CONTAINER, new_term))
        frames = {k: json.loads(v)
                  for k, v in storage.get_container_items(Index.FRAMES_CONTAINER, old_positions.keys()).items()}
        # Update term positions globally and positions and associations in frames
        for frame_id, positions in old_positions.items():
            # Update global positions
            try:
                new_positions[frame_id].extend(positions)
            except KeyError:
                # New term had not been recorded in this frame
                new_positions[frame_id] = positions
            # Update frame positions
            try:
                frames[frame_id]['_positions'][new_term].extend(positions)
            except KeyError:
                # New term had not been recorded in this frame
                frames[frame_id]['_positions'][new_term] = positions
            # Update frame associations
            frames[frame_id]['_associations'].clear()
            for term in frames[frame_id]['_positions']:
                for other_term in frames[frame_id]['_positions']:
                    if other_term == term:
                        continue
                    try:
                        frames[frame_id]['_associations'][term].append(other_term)
                    except KeyError:
                        frames[frame_id]['_associations'][term] = [other_term]
            # Kludge! JSON doesn't support sets....
            for term, asscs in frames[frame_id]['_associations'].items():
                frames[frame_id]['_associations'][term] = set(asscs)
            del frames[frame_id]['_positions'][old_term]
        storage.set_container_items(Index.FRAMES_CONTAINER, {f_id: json.dumps(d) for f_id, d in frames.items()})
        storage.set_container_item(Index.POSITIONS_CONTAINER, new_term,
                                   json.dumps({k: v for k, v in new_positions.items()}))
        storage.delete_container_item(Index.POSITIONS_CONTAINER, old_term)

        # Update term associations

        if old_term in associations:
            for other_term, freq in associations[old_term].items():
                # Update new term
                try:
                    associations[new_term][other_term] += freq
                except KeyError:
                    # New term had no recorded associations with other term
                    associations[new_term][other_term] = freq
                # Update other term
                try:
                    associations[other_term][new_term] += associations[other_term][old_term]
                except KeyError:
                    # Other term had no recorded associations with new term
                    associations[other_term][new_term] = associations[other_term][old_term]
                del associations[other_term][old_term]
            del associations[old_term]

        # Update term frequency
        freqs = {k: json.loads(v) if v else 0
                 for k, v in storage.get_container_items(Index.FREQUENCIES_CONTAINER, [old_term, new_term]).items()}
        storage.set_container_item(Index.FREQUENCIES_CONTAINER, new_term, sum(freqs.values()))
        storage.delete_container_item(Index.FREQUENCIES_CONTAINER, old_term)

    def run_plugin(self, cls, **args):
        """
        Runs an ``plugin.AnalyticsPlugin`` on this index. Creates an instance of the plugin by passing it this object
        before calling its run method and saving the result into container(s) prefixed with the plugins name.

        Any keyword arguments passed to this method in args are passed onto the run method of the plugin.

        Required Arguments:
        cls -- a python class that extends ``plugin.AnalyticsPlugin``

        Optional Arguments:
        **args -- keywords args that are passed onto the run method of an instance of cls.

        """
        plugin = cls(self)
        logger.info('Running {} plugin.'.format(plugin.get_name()))
        result = plugin.run(**args)

        for container, value in result.items():
            # Max sure the container exists and is clear
            container_id = "{}:{}".format(plugin.get_name(), container)
            try:
                self._storage.add_container(container_id)
            except DuplicateContainerError:
                self._storage.clear_container(container_id)

            # Make sure items are seralised
            for key, data in value.items():
                value[key] = json.dumps(value[key])

            # Store items
            self._storage.set_container_items(container_id, value)

    def get_plugin_data(self, plugin, container):
        """
        Returns a container identified by container for the plugin instance.

        Required Arguments:
        plugin -- an instance of ``plugin.AnalyticsPlugin``.
        container -- The str name of the container.

        """
        return self._storage.get_container_items("{}:{}".format(plugin.get_name(), container))


def find_bi_gram_words(frames, min_bi_gram_freq=3, min_bi_gram_coverage=0.65):
    """
    This function finds bi-gram words from the specified ``Frame``s iterable which pass certain criteria.

    This function uses a ``PotentialBiGramAnalyser`` to identify potential bi-grams. Names and stopwords are not
    considered for bi-grams. Only bi-grams that have a certain frequency and coverage are considered.

    Required Arguments:
    frames -- An iterable of ``Frame`` objects for textual data

    Optional Arguments:
    min_bi_gram_freq -- A int representing the minimum frequency cutoff for each bi_gram
    min_bi_gram_coverage -- A float representing the cutoff ratio for bi_gram frequency over individual word frequencies

    Returns a list of bi-gram strings that pass the criteria.

    """
    logger.info("Identifying n-grams")

    # Generate a table of candidate bigrams
    candidate_bi_grams = nltk.probability.FreqDist()
    uni_gram_frequencies = nltk.probability.FreqDist()
    bi_gram_analyser = PotentialBiGramAnalyser()
    for frame in frames:
        for sentence in frame.sentences:
            terms_seen = set()
            for token_list in bi_gram_analyser.analyse(sentence):
                # Using a special filter that returns list of tokens. List of 1 means no bi-grams.
                if len(token_list) > 1:  # We have a bi-gram people!
                    candidate_bi_grams.inc("{} {}".format(token_list[0].value, token_list[1].value))
                for t in token_list:  # Keep a list of terms we have seen so we can record freqs later.
                    if not t.stopped:  # Naughty stopwords!
                        terms_seen.add(t.value)
            for term in terms_seen:
                uni_gram_frequencies.inc(term)

    # Filter and sort by frequency-decreasing
    candidate_bi_gram_list = filter(lambda (k, v): v > min_bi_gram_freq, candidate_bi_grams.items())
    candidate_bi_gram_list = filter(lambda (k, v): v / uni_gram_frequencies[k.split(" ")[0]] > min_bi_gram_coverage
                                    and v / uni_gram_frequencies[k.split(" ")[1]] > min_bi_gram_coverage,
                                    candidate_bi_gram_list)
    logger.info("Identified {} n-grams.".format(len(candidate_bi_gram_list)))

    return [b[0] for b in candidate_bi_gram_list]
