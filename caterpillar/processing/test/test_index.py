# caterpillar: Tests for the caterpillar.processing.index module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@mammothlabs.com.au)
from __future__ import division
import os
import pytest
from caterpillar.data.storage import DocumentNotFoundError
from caterpillar.processing.analysis.analyse import DefaultTestAnalyser, BiGramTestAnalyser

from caterpillar.processing.frames import frame_stream
from caterpillar.processing.index import *
from caterpillar.processing.schema import TEXT, Schema


# Boilerplate and plumbing
def test_index_open():
    with pytest.raises(NotImplementedError):
        Index.open("", RamStorage)


def test_index_destroy():
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                        document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)))
        index.add_document(text=data, document='alice.txt', frame_size=2, fold_case=False)
        index.destroy()

        with pytest.raises(KeyError):
            index.get_associations_index()


def test_index_searcher():
    with pytest.raises(NotImplementedError):
        index = Index.create(Schema(text=TEXT))
        index.searcher()


# Functional tests
def test_index_alice():
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)))
        doc_id = index.add_document(text=data, document='alice.txt', frame_size=2, fold_case=False, update_index=True)

        assert len(index.get_term_positions('nice')) == 3
        assert len(index.get_term_positions('key')) == 5

        assert index.get_term_association('Alice', 'poor') == index.get_term_association('poor', 'Alice') == 3
        assert index.get_term_association('key', 'golden') == index.get_term_association('golden', 'key') == 3

        assert len(index.get_frequencies()) == 504
        assert index.get_term_frequency('Alice') == 23

        index.delete_document(doc_id)

        with pytest.raises(DocumentNotFoundError):
            index.get_document(doc_id)


def test_index_frames_docs_alice():
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)))
        index.add_document(text=data, document='alice.txt', frame_size=2, fold_case=False, update_index=True)

        assert index.get_frame_count() == 49

        frame_id = index.get_term_positions('Alice').keys()[0]
        assert frame_id == index.get_frame(frame_id)['_id']

        doc_id = frame_id.split('-')[0]
        assert doc_id == index.get_document(doc_id)['_id']


def test_index_moby_small():
    with open(os.path.abspath('caterpillar/resources/moby_small.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser())))
        index.add_document(text=data, frame_size=2, fold_case=False)

        assert len(index.get_term_positions('Mr. Chace')) == 1
        assert len(index.get_term_positions('CONVERSATIONS')) == 1
        assert len(index.get_frequencies()) == 38

def test_index_alice_with_bigram_words():
    with open(os.path.abspath('caterpillar/resources/alice.txt'), 'r') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=BiGramTestAnalyser(bi_grams))))
        index.add_document(text=data, frame_size=2, fold_case=False)

        assert len(bi_grams) == 5
        assert 'golden key' in bi_grams
        assert index.get_term_frequency('golden key') == 6
        assert index.get_term_frequency('golden') == 1
        assert index.get_term_frequency('key') == 3


def test_index_moby_with_case_folding():
    with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser())))
        index.add_document(text=data, frame_size=2, fold_case=True, update_index=True)

        with pytest.raises(KeyError):
            index.get_term_positions('flask')
        with pytest.raises(KeyError):
            assert not index.get_term_frequency('flask')
        assert index.get_term_frequency('Flask') == 92
        assert index.get_term_association('Flask', 'person') == index.get_term_association('person', 'Flask') == 3

        with pytest.raises(KeyError):
            index.get_term_positions('Well')
        with pytest.raises(KeyError):
            assert not index.get_term_frequency('Well')
        assert index.get_term_frequency('well') == 208
        assert index.get_term_association('well', 'whale') == index.get_term_association('whale', 'well') == 26

        with pytest.raises(KeyError):
            index.get_term_positions('Whale')
        with pytest.raises(KeyError):
            assert not index.get_term_frequency('Whale')
        assert index.get_term_frequency('whale') == 811
        assert index.get_term_association('whale', 'American') == index.get_term_association('American', 'whale') == 15

        assert index.get_term_frequency('T. HERBERT') == 1
        assert len(index.get_frequencies()) == 17913


def test_index_alice_with_case_folding():
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                             document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)))
        index.add_document(text=data, document='alice.txt', frame_size=2, fold_case=False, update_index=False)
        index.reindex(fold_case=True)

        positions_index = index.get_positions_index()
        frames = {k: json.loads(v) for k, v in index._storage.get_container_items(Index.FRAMES_CONTAINER).items()}
        for frame_id, frame in frames.items():
            for term in frame['_positions']:
                assert frame_id in positions_index[term]


def test_find_bigram_words():
    with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=BiGramTestAnalyser(bi_grams))))
        index.add_document(text=data, frame_size=2, fold_case=True, update_index=True)

        assert len(bi_grams) == 3
        assert 'vinegar cruet' in bi_grams
        assert index.get_term_frequency('vinegar cruet') == 4
        with pytest.raises(KeyError):
            index.get_term_positions('vinegar')
        with pytest.raises(KeyError):
            index.get_term_positions('cruet')
