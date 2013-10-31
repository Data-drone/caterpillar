# caterpillar: Tests for the caterpillar.searching
#
# Copyright (C) 2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import csv
import os

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.processing.analysis.analyse import BiGramTestAnalyser
from caterpillar.processing.index import Index, find_bi_gram_words
from caterpillar.processing import schema
from caterpillar.processing.frames import frame_stream
from caterpillar.searching.scoring import SimpleScorer


def test_searching_alice():
    """Test basic searching functions for Alice."""
    with open(os.path.abspath('caterpillar/resources/alice.txt'), 'rbU') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        index = Index.create(schema.Schema(
            text=schema.TEXT(analyser=BiGramTestAnalyser(bi_grams))))
        index.add_document(text=data, frame_size=2, fold_case=True)
        index.run_plugin(InfluenceAnalyticsPlugin,
                         influence_contribution_threshold=0,
                         cumulative_influence_smoothing=False)

        searcher = index.searcher()

        assert searcher.count("King") == searcher.count("K?ng") == 60

        assert searcher.count("Queen or K??g") == 123 == searcher.count("King or Queen")
        assert searcher.count("King AND Queen") == 4
        assert searcher.count("King NOT Queen") == 56
        assert searcher.count('"golden key"') == 6
        assert searcher.count('*ing') == 514

        assert "jury" in searcher.search("jury", limit=1)[0].text

        assert searcher.count("Alice and (thought or little)") == \
            searcher.count("Alice and thought or Alice and little") == 95

        assert searcher.count("thistermdoesntexist") == 0

        voice_hits = searcher.count("voice")
        assert voice_hits == 47

        results = searcher.search("Alice or voice^1.5", limit=voice_hits)
        for hit in results:
            assert "voice" in hit.text

        results = searcher.search("Alice or voice^1.5", start=voice_hits)
        for hit in results:
            assert "voice" not in hit.text

        results = searcher.search("King not (court or evidence)")
        assert len(results.term_weights) == 1
        assert len(results) == 25
        assert results.num_matches == 53
        for hit in results:
            assert "evidence" not in hit.text
            assert "court" not in hit.text

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
        index.run_plugin(InfluenceAnalyticsPlugin,
                         influence_contribution_threshold=0,
                         cumulative_influence_smoothing=False)

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
        index.run_plugin(InfluenceAnalyticsPlugin,
                         influence_contribution_threshold=0,
                         cumulative_influence_smoothing=False)

        searcher = index.searcher()

        assert searcher.count('1770') == 2
        assert searcher.count('1,900') == 1
        assert searcher.count('4.4') == 1


def test_searching_twitter():
    """Test searching twitter data."""
    with open('caterpillar/resources/twitter_sentiment.csv', 'rbU') as f:
        index = Index.create(schema.Schema(text=schema.TEXT))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(text=row[1])
        index.reindex()
        index.run_plugin(InfluenceAnalyticsPlugin,
                         influence_contribution_threshold=0,
                         cumulative_influence_smoothing=False)

        searcher = index.searcher()

        assert searcher.count('@NYSenate') == 1
        assert searcher.count('summerdays@gmail.com') == 1
