# caterpillar: Tests for the caterpillar.searching
#
# Copyright (C) 2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import csv
import os

import pytest

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin, InfluenceTopicsPlugin
from caterpillar.processing.analysis.analyse import BiGramTestAnalyser
from caterpillar.processing.index import Index, find_bi_gram_words
from caterpillar.processing import schema
from caterpillar.processing.frames import frame_stream
from caterpillar.searching.query import QueryError
from caterpillar.searching.scoring import SimpleScorer


def test_searching_alice():
    """Test basic searching functions for Alice."""
    with open(os.path.abspath('caterpillar/resources/alice.txt'), 'rbU') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(schema.Schema(text=schema.TEXT(analyser=BiGramTestAnalyser(bi_grams))))
        index.add_document(text=data, frame_size=2, fold_case=True)
        index.run_plugin(InfluenceAnalyticsPlugin, influence_factor_smoothing=False)
        topics_plugin = index.run_plugin(InfluenceTopicsPlugin)
        topics = topics_plugin.get_topical_classification().topics

        searcher = index.searcher()

        for topic in topics:
            if topic.name == 'Queen':
                assert searcher.count(topic.get_query()) == 70

        assert searcher.count("King") == searcher.count("K?ng") == 60

        assert searcher.count("Queen or K??g") == 123 == searcher.count("King or Queen")
        assert searcher.count("King AND Queen") == 4
        assert searcher.count("King NOT Queen") == 56
        assert searcher.count('golden key') == 6
        assert searcher.count('*ing') == 514

        assert "jury" in searcher.search("jury", limit=1)[0].data['text']

        assert searcher.count("Alice and (thought or little)") == \
            searcher.count("Alice and thought or Alice and little") == 95

        assert searcher.count("thistermdoesntexist") == 0

        voice_hits = searcher.count("voice")
        assert voice_hits == 47

        results = searcher.search("Alice or voice^1.5", limit=voice_hits)
        for hit in results:
            assert "voice" in hit.data['text']

        results = searcher.search("Alice or voice^1.5", start=voice_hits)
        for hit in results:
            assert "voice" not in hit.data['text']

        results = searcher.search("King not (court or evidence)")
        assert len(results.term_weights) == 1
        assert len(results) == 25
        assert results.num_matches == 53
        for hit in results:
            assert "evidence" not in hit.data['text']
            assert "court" not in hit.data['text']

        assert searcher.count('Mock Turtle') == 51

        assert searcher.count('*t? R*b??') == searcher.count('White Rabbit')


def test_searching_alice_simple():
    """Test searching for Alice with the simple scorer."""
    with open(os.path.abspath('caterpillar/resources/alice.txt'), 'rbU') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(schema.Schema(
            text=schema.TEXT(analyser=BiGramTestAnalyser(bi_grams))))
        index.add_document(text=data, frame_size=2, fold_case=True)
        index.run_plugin(InfluenceAnalyticsPlugin, influence_factor_smoothing=False)

        searcher = index.searcher(scorer_cls=SimpleScorer)

        results = searcher.search('Alice or Caterpillar')
        assert results[0].score == 2


def test_searching_mt_warning():
    """Test searching for mt warning data."""
    with open(os.path.abspath('caterpillar/resources/mt_warning_utf8.txt'), 'rbU') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(schema.Schema(
            text=schema.TEXT(analyser=BiGramTestAnalyser(bi_grams))))
        index.add_document(text=data, frame_size=2, fold_case=True)
        index.run_plugin(InfluenceAnalyticsPlugin, influence_factor_smoothing=False)

        searcher = index.searcher()

        assert searcher.count('1770') == 2
        assert searcher.count('1,900') == 1
        assert searcher.count('4.4') == 1

        assert searcher.count('*') == index.get_frame_count()


def test_searching_twitter():
    """Test searching twitter data."""
    with open('caterpillar/resources/twitter_sentiment.csv', 'rbU') as f:
        index = Index.create(schema.Schema(text=schema.TEXT, sentiment=schema.CATEGORICAL_TEXT(indexed=True)))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(text=row[1], sentiment=row[0])
        index.reindex()
        index.run_plugin(InfluenceAnalyticsPlugin, influence_factor_smoothing=False)

        searcher = index.searcher()

        assert searcher.count('@NYSenate') == 1
        assert searcher.count('summerdays@gmail.com') == 1

        assert searcher.count('sentiment=positive') + searcher.count('sentiment=negative') == index.get_frame_count()


def test_searching_nps():
    """Test searching nps-backed data."""
    with open('caterpillar/resources/big.csv', 'rbU') as f:
        index = Index.create(schema.Schema(respondant=schema.NUMERIC, region=schema.CATEGORICAL_TEXT(indexed=True),
                                           store=schema.CATEGORICAL_TEXT(indexed=True), liked=schema.TEXT,
                                           disliked=schema.TEXT, would_like=schema.TEXT,
                                           nps=schema.NUMERIC(indexed=True), fake=schema.NUMERIC(indexed=True),
                                           fake2=schema.CATEGORICAL_TEXT(indexed=True)))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(update_index=False, respondant=row[0], region=row[1], store=row[2], liked=row[3],
                               disliked=row[4], would_like=row[5], nps=row[6], fake2=None)

        index.reindex()
        index.run_plugin(InfluenceAnalyticsPlugin, influence_factor_smoothing=False)

        searcher = index.searcher()

        assert searcher.count('nps=10 and store=DANNEVIRKE') == 6

        docs = set()
        results = searcher.search('region=Otago and nps<5')
        for hit in results:
            docs.add(hit.doc_id)
        assert len(docs) == 5
        assert len(results) == 15

        num_christchurch = searcher.count('region=Christchurch')
        num_null_nps_christchurch = num_christchurch - searcher.count('region=Christchurch and nps > 0')
        assert num_christchurch == searcher.count('region=Christchurch and nps < 8') \
            + searcher.count('region=Christchurch and nps >= 8') \
            + num_null_nps_christchurch
        assert searcher.count('region=Christchurch and nps>7 and (reliable or quick)') \
            == searcher.count('region = Christchurch and nps>7') \
            - searcher.count('region=Christchurch and nps > 7 not (reliable or quick)')

        assert searcher.count('nps>0') == searcher.count('nps<=7') + searcher.count('nps>7')

        assert searcher.count('region=Christ*') == num_christchurch == 1399

        with pytest.raises(QueryError):
            searcher.count('nps >= 1?')
        with pytest.raises(QueryError):
            searcher.count('nps=?')
        with pytest.raises(QueryError):
            searcher.count('n*s=10')
        with pytest.raises(QueryError):
            searcher.count('badfield=something')
        with pytest.raises(QueryError):
            searcher.count('region>something')
        with pytest.raises(QueryError):
            searcher.count('region>=something')
        with pytest.raises(QueryError):
            searcher.count('region<something')
        with pytest.raises(QueryError):
            searcher.count('region<=something')
        with pytest.raises(QueryError):
            searcher.count('liked=something')
        with pytest.raises(QueryError):
            searcher.count('respondant=something')
        with pytest.raises(QueryError):
            searcher.count('region>Christchurch')
        with pytest.raises(QueryError):
            searcher.count('nps>bad')

        assert searcher.count('fake=1') == 0
        assert searcher.count('fake2=something') == 0
        assert searcher.count('region=nonexistentregion') == 0


def test_searching_nps_no_text_update():
    """Test retrieving by ID with no text."""
    with open('caterpillar/resources/big.csv', 'rbU') as f:
        index = Index.create(schema.Schema(respondant=schema.ID(indexed=True),
                                           region=schema.CATEGORICAL_TEXT(indexed=True), nps=schema.NUMERIC))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(update_index=False, respondant=row[0], region=row[1], nps=row[6])
        index.reindex(update_only=True)
        searcher = index.searcher()

        assert searcher.count('respondant = 1') == 1
        assert searcher.count('region = Chr*') == 878  # Previous test gets 1399 for this because of text frame splits.


def test_no_data_by_circumstance():
    """Test that when we add data with no indexed fields we can't retrieve it."""
    with open('caterpillar/resources/test_small.csv', 'rbU') as f:
        index = Index.create(schema.Schema(respondant=schema.ID, nps=schema.NUMERIC))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(update_index=False, respondant=row[0], region=row[1], nps=row[6])
        index.reindex(update_only=True)
        searcher = index.searcher()

        assert searcher.count('*') == 0
