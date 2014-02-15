# caterpillar: Tests for the caterpillar.processing.index module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>, Ryan Stuart <ryan@mammothlabs.com.au)
from __future__ import division
import csv
import os
import tempfile
import pytest

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.data.sqlite import SqliteStorage, SqliteMemoryStorage
from caterpillar.processing.analysis.analyse import DefaultTestAnalyser, BiGramTestAnalyser, EverythingAnalyser
from caterpillar.processing.frames import frame_stream
from caterpillar.processing.index import *
from caterpillar.processing.schema import ID, NUMERIC, CATEGORICAL_TEXT, TEXT, FieldType, Schema


STORAGE = [(SqliteStorage), (SqliteMemoryStorage)]
FAST_STORAGE = [(SqliteMemoryStorage)]


@pytest.fixture(scope="function", autouse=True)
def delete_databases():
    if os.path.isfile(Index.DATA_STORAGE):
        os.remove(Index.DATA_STORAGE)
    if os.path.isfile(Index.RESULTS_STORAGE):
        os.remove(Index.RESULTS_STORAGE)


@pytest.mark.parametrize("storage_cls", STORAGE)
def test_index_destroy(storage_cls):
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)),
                             storage_cls=storage_cls, path=os.getcwd())
        index.add_document(text=data, document='alice.txt', frame_size=2, fold_case=False)
        index.destroy()


def test_index_open():
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False),
                                    flag=FieldType(analyser=EverythingAnalyser(), indexed=True, categorical=True)),
                             storage_cls=SqliteStorage, path=os.getcwd())
        index.add_document(text=data, document='alice.txt', flag=True, frame_size=2, fold_case=False)
        index = Index.open(os.getcwd(), SqliteStorage)
        index.reindex()
        assert len(index.get_frequencies()) == 504
        assert index.get_term_frequency('Alice') == 23
        assert index.get_document_count() == 1
        assert isinstance(index.get_schema()['text'], TEXT)
        assert index.is_derived() is False
        index.destroy()

    with pytest.raises(IndexNotFoundError):
        Index.open("fake", SqliteStorage)


# Functional tests
@pytest.mark.parametrize("storage_cls", STORAGE)
def test_index_alice(storage_cls):
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False),
                                    blank=NUMERIC(indexed=True), ref=ID(indexed=True)),
                             storage_cls=storage_cls, path=os.getcwd())
        doc_id = index.add_document(text=data, document='alice.txt', blank=None, ref=123, frame_size=2, fold_case=False,
                                    update_index=True)

        assert len(index.get_term_positions('nice')) == 3
        assert len(index.get_term_positions('key')) == 5

        assert index.get_term_association('Alice', 'poor') == index.get_term_association('poor', 'Alice') == 3
        assert index.get_term_association('key', 'golden') == index.get_term_association('golden', 'key') == 3

        assert index.get_vocab_size() == len(index.get_frequencies()) == 504
        assert index.get_term_frequency('Alice') == 23
        assert index.__sizeof__() == index.get_frame_count() * 10 * 1024

        index.delete_document(doc_id, update_index=True)

        with pytest.raises(DocumentNotFoundError):
            index.get_document(doc_id)

        assert not 'Alice' in index.get_frequencies()
        assert not 'Alice' in index.get_associations_index()
        assert not 'Alice' in index.get_positions_index()


@pytest.mark.parametrize("storage_cls", STORAGE)
def test_index_frames_docs_alice(storage_cls):
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)),
                             storage_cls=storage_cls, path=os.getcwd())
        index.add_document(text=data, document='alice.txt', frame_size=2, fold_case=False, update_index=True)

        assert index.get_frame_count() == 49

        frame_id = index.get_term_positions('Alice').keys()[0]
        assert frame_id == index.get_frame(frame_id)['_id']

        doc_id = frame_id.split('-')[0]
        assert doc_id == index.get_document(doc_id)['_id']


@pytest.mark.parametrize("storage_cls", STORAGE)
def test_index_moby_small(storage_cls):
    with open(os.path.abspath('caterpillar/resources/moby_small.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser())),
                             storage_cls=storage_cls, path=os.getcwd())
        index.add_document(text=data, frame_size=2, fold_case=False)

        assert len(index.get_term_positions('Mr. Chace')) == 1
        assert len(index.get_term_positions('CONVERSATIONS')) == 1
        assert len(index.get_frequencies()) == 38


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_index_alice_bigram_words(storage_cls):
    with open(os.path.abspath('caterpillar/resources/alice.txt'), 'r') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=BiGramTestAnalyser(bi_grams))),
                             storage_cls=storage_cls, path=os.getcwd())
        index.add_document(text=data, frame_size=2, fold_case=False)

        assert len(bi_grams) == 5
        assert 'golden key' in bi_grams
        assert index.get_term_frequency('golden key') == 6
        assert index.get_term_frequency('golden') == 1
        assert index.get_term_frequency('key') == 3


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_index_moby_case_folding(storage_cls):
    with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser())),
                             storage_cls=storage_cls, path=os.getcwd())
        index.add_document(text=data, frame_size=2, fold_case=True, update_index=True)

        with pytest.raises(KeyError):
            index.get_term_positions('flask')
        with pytest.raises(KeyError):
            assert not index.get_term_frequency('flask')
        assert index.get_term_frequency('Flask') == 91
        assert index.get_term_association('Flask', 'person') == index.get_term_association('person', 'Flask') == 2

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
        assert index.get_term_frequency('whale') == 803
        assert index.get_term_association('whale', 'American') == index.get_term_association('American', 'whale') == 14

        assert index.get_term_frequency('T. HERBERT') == 1
        assert len(index.get_frequencies()) == 17913


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_index_alice_case_folding(storage_cls):
    with open(os.path.abspath('caterpillar/resources/alice.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                             document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)),
                             storage_cls=storage_cls, path=os.getcwd())
        index.add_document(text=data, document='alice.txt', frame_size=2, fold_case=False, update_index=False)
        index.reindex(fold_case=True)

        positions_index = index.get_positions_index()
        frames = {k: json.loads(v) for k, v in index._data_storage.get_container_items(Index.FRAMES_CONTAINER).items()}
        for frame_id, frame in frames.items():
            for term in frame['_positions']:
                assert frame_id in positions_index[term]

        # Check that associations never exceed frequency of either term
        associations = index.get_associations_index()
        frequencies = index.get_frequencies()
        for term, term_associations in associations.items():
            for other_term, assoc in term_associations.items():
                assert assoc <= frequencies[term] and assoc <= frequencies[other_term]

        # Check that global associations index matches frame associations
        for term, term_positions in positions_index.items():
            term_associations = {}
            for frame_id in term_positions:
                frame = frames[frame_id]
                if len(frame['_associations']) == 0:
                    # Skip frame with no associations
                    continue
                for other_term in frame['_associations'][term]:
                    try:
                        term_associations[other_term] += 1
                    except KeyError:
                        term_associations[other_term] = 1
            for other_term in term_associations:
                assert term_associations[other_term] == associations[term][other_term]\
                    == associations[other_term][term]

        # Check frequencies against positions
        frequencies = index.get_frequencies()
        for term, freq in frequencies.items():
            assert freq == len(positions_index[term])


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_find_bigram_words(storage_cls):
    with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=BiGramTestAnalyser(bi_grams))),
                             storage_cls=storage_cls, path=os.getcwd())
        index.add_document(text=data, frame_size=2, fold_case=True, update_index=True)

        assert len(bi_grams) == 3
        assert 'vinegar cruet' in bi_grams
        assert index.get_term_frequency('vinegar cruet') == 4
        with pytest.raises(KeyError):
            index.get_term_positions('vinegar')
        with pytest.raises(KeyError):
            index.get_term_positions('cruet')


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_utf8(storage_cls):
    with open(os.path.abspath('caterpillar/resources/mt_warning_utf8.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)),
                             storage_cls=storage_cls, path=os.getcwd())
        doc_id = index.add_document(text=data, document='mt_warning_utf8.txt', frame_size=2, fold_case=False,
                                    update_index=True)
        assert doc_id


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_latin1(storage_cls):
    with open(os.path.abspath('caterpillar/resources/mt_warning_latin1.txt'), 'r') as f:
        data = f.read()
        index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                    document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)),
                             storage_cls=storage_cls, path=os.getcwd())
        doc_id = index.add_document(text=data, document='mt_warning_latin1.txt', frame_size=2, fold_case=False,
                                    update_index=True,
                                    encoding='latin1')
        assert doc_id


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_encoding(storage_cls):
    index = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser())))
    doc_id = index.add_document(text=u'This is a unicode string to test our field decoding.', frame_size=2,
                                fold_case=False, update_index=True)
    assert doc_id is not None

    with open(os.path.abspath('caterpillar/resources/mt_warning_utf8.txt'), 'r') as f:
        data = f.read()
    with pytest.raises(IndexError):
        doc_id = index.add_document(text=data, frame_size=2, fold_case=False, update_index=True, encoding='ascii')


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_derived_index_composite(storage_cls):
    temp1 = tempfile.mkdtemp()
    temp2 = tempfile.mkdtemp()
    try:
        with open(os.path.abspath('caterpillar/resources/detractors.csv'), 'rbU') as f:
            index1 = Index.create(Schema(text=TEXT), storage_cls=storage_cls, path=temp1)
            csv_reader = csv.reader(f)
            for row in csv_reader:
                index1.add_document(update_index=False, text=row[0])
            index1.reindex()
            scount1 = index1.searcher().count("service")
            nscount1 = index1.searcher().count("* not service")

        with open(os.path.abspath('caterpillar/resources/promoters.csv'), 'rbU') as f:
            index2 = Index.create(Schema(text=TEXT), storage_cls=storage_cls, path=temp2)
            csv_reader = csv.reader(f)
            for row in csv_reader:
                index2.add_document(update_index=False, text=row[0])
            index2.reindex()
            scount2 = index2.searcher().count("service")
            nscount2 = index2.searcher().count("* not service")

        index = DerivedIndex.create_from_composite_query([(index1, "service"), (index2, "service")],
                                                         storage_cls=storage_cls, path=os.getcwd())

        searcher = index.searcher()
        assert searcher.count("service") == scount1 + scount2
        assert searcher.count("*") == (index1.get_frame_count() - nscount1) + (index2.get_frame_count() - nscount2)
        assert index.is_derived() is True

        with pytest.raises(NotImplementedError):
            index.add_document(text='text')
        with pytest.raises(NotImplementedError):
            index.delete_document(None)
        with pytest.raises(NotImplementedError):
            index.get_document(None)
        with pytest.raises(NotImplementedError):
            index.get_document_count()

        index1.destroy()
        index2.destroy()
    finally:
        os.rmdir(temp1)
        os.rmdir(temp2)


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_derived_index_asymmetric_schema(storage_cls):
    temp1 = tempfile.mkdtemp()
    temp2 = tempfile.mkdtemp()
    try:
        with open(os.path.abspath('caterpillar/resources/mt_warning_utf8.txt'), 'r') as f:
            data = f.read()
            index1 = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                         document=TEXT(analyser=DefaultTestAnalyser(), indexed=False)),
                                  storage_cls=storage_cls, path=temp1)
            index1.add_document(text=data, document='mt_warning_utf8.txt', frame_size=2, fold_case=False,
                                update_index=True)

        with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
            data = f.read()
            index2 = Index.create(Schema(text=TEXT(analyser=DefaultTestAnalyser()),
                                         document=TEXT(analyser=DefaultTestAnalyser(), indexed=False), marker=NUMERIC),
                                  storage_cls=storage_cls, path=temp2)
            index2.add_document(text=data, document='alice.txt', marker=777, frame_size=2, fold_case=False,
                                update_index=True)

        q1 = "mountain or rock or volcanic or volcano"
        q2 = "Alice or King or Queen"
        index = DerivedIndex.create_from_composite_query([(index1, q1), (index2, q2)],
                                                         storage_cls=storage_cls, path=os.getcwd())

        searcher = index.searcher()
        c1 = index1.searcher().count(q1)
        c2 = index2.searcher().count(q2)
        assert searcher.count(q1) == c1
        assert searcher.count(q2) == c2
        assert searcher.count("*") == index.get_frame_count() == c1 + c2

        assert searcher.search(q2, limit=1)[0].data['marker'] == 777
        assert 'marker' not in searcher.search(q1, limit=1)[0].data

        index1.destroy()
        index2.destroy()
    finally:
        os.rmdir(temp1)
        os.rmdir(temp2)


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_index_update(storage_cls):
    with open(os.path.abspath('caterpillar/resources/detractors.csv'), 'rbU') as f:
        index = Index.create(Schema(text=TEXT), storage_cls=storage_cls)
        csv_reader = csv.reader(f)
        for row in csv_reader:
            index.add_document(update_index=False, text=row[0])
        index.reindex(update_only=True, fold_case=False)

    assert index.get_term_frequency('service') == 14

    with open(os.path.abspath('caterpillar/resources/promoters.csv'), 'rbU') as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            index.add_document(update_index=False, text=row[0])
        index.reindex(update_only=True, fold_case=False)

    assert index.get_term_frequency('service') == 65
    assert index.get_frame_count() == 534

    index.reindex(fold_case=False, update_only=False)

    assert index.get_term_frequency('service') == 65
    assert index.get_frame_count() == 534


@pytest.mark.parametrize("storage_cls", FAST_STORAGE)
def test_index_state(storage_cls):
    with open(os.path.abspath('caterpillar/resources/detractors.csv'), 'rbU') as f:
        index = Index.create(Schema(text=TEXT), storage_cls=storage_cls)
        start_revision = index.get_revision()
        csv_reader = csv.reader(f)
        doc_ids = []
        for row in csv_reader:
            doc_ids.append(index.add_document(update_index=False, text=row[0]))
            assert start_revision != index.get_revision()
        assert not index.is_clean()
        index.reindex()
        assert index.is_clean()
        revision = index.get_revision()
        index.delete_document(doc_ids[0], update_index=False)
        assert revision != index.get_revision()
        assert not index.is_clean()
        index.reindex()
        assert index.is_clean()
