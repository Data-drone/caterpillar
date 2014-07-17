# Copyright (C) Kapiche
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""Tests for the searching package."""
import csv
import os

import pytest

from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.analyse import DefaultAnalyser, BiGramAnalyser
from caterpillar.processing.index import Index, find_bi_gram_words
from caterpillar.processing import schema
from caterpillar.processing.frames import frame_stream
from caterpillar.searching.query import QueryError
from caterpillar.searching.query.boolean import MatchAllQuery, MatchSomeQuery
from caterpillar.searching.query.querystring import QueryStringQuery as QSQ
from caterpillar.searching.scoring import SimpleScorer


def test_searching_alice():
    """Test basic searching functions for Alice."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        analyser = BiGramAnalyser(bi_grams, stopword_list=stopwords.ENGLISH_TEST)
        index = Index.create(schema.Schema(text=schema.TEXT(analyser=analyser)))
        index.add_document(text=data, frame_size=2)
        index.fold_term_case()
        searcher = index.searcher()

        assert searcher.count(QSQ("King")) == searcher.count(QSQ("K?ng"))
        assert searcher.count(QSQ("Queen or K??g")) == 123 == \
            searcher.count(QSQ("King or Queen"))
        assert searcher.count(QSQ("King AND Queen")) == 4 == \
            searcher.count(MatchAllQuery([QSQ('King'), QSQ('Queen')])) == \
            searcher.count(QSQ('King')) - searcher.count(QSQ('King not Queen'))
        assert searcher.count(QSQ("King NOT Queen")) == 56
        assert searcher.count(QSQ('golden key')) == 6
        assert searcher.count(QSQ('*ing')) == 514
        assert searcher.count(QSQ("Alice and (thought or little)")) == \
            searcher.count(QSQ("Alice and thought or Alice and little")) == 95 == \
            searcher.count(MatchAllQuery([QSQ('Alice'), MatchSomeQuery([QSQ('thought'), QSQ('little')])]))
        assert searcher.count(QSQ("thistermdoesntexist")) == 0
        assert searcher.count(QSQ('Mock Turtle')) == 51
        assert searcher.count(QSQ('*t? R*b??')) == searcher.count(QSQ('White Rabbit'))

        assert "jury" in searcher.search(QSQ("jury"), limit=1)[0].data['text']

        voice_hits = searcher.count(QSQ("voice"))
        assert voice_hits == 47
        results = searcher.search(QSQ("Alice or voice^1.5"), limit=voice_hits)
        for hit in results:
            assert "voice" in hit.data['text']
        results = searcher.search(QSQ("Alice or voice^1.5"), start=voice_hits)
        for hit in results:
            assert "voice" not in hit.data['text']

        results = searcher.search(QSQ("King not (court or evidence)"))
        assert len(results) == 25
        assert len(results.term_weights) == 1
        assert results.num_matches == 53 == searcher.count(MatchAllQuery([QSQ('King')], [QSQ('court or evidence')]))
        for hit in results:
            assert "evidence" not in hit.data['text']
            assert "court" not in hit.data['text']

        # Check multiple boostings; this example is totally contrived but a real case could occur when combining
        # different plugin queries.
        results = searcher.search(MatchSomeQuery([QSQ("King"), QSQ("court AND King^1.5")]))
        assert results.term_weights['King'] == 1.5

        with pytest.raises(TypeError):
            # Invalid query format
            searcher.count('hello')


def test_searching_alice_simple():
    """Test searching for Alice with the simple scorer."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        analyser = BiGramAnalyser(bi_grams, stopword_list=stopwords.ENGLISH_TEST)
        index = Index.create(schema.Schema(text=schema.TEXT(analyser=analyser)))
        index.add_document(text=data, frame_size=2)
        searcher = index.searcher(scorer_cls=SimpleScorer)
        results = searcher.search(QSQ('Alice or Caterpillar'))
        assert results[0].score == 2


def test_searching_mt_warning():
    """Test searching for mt warning data."""
    with open(os.path.abspath('caterpillar/test_resources/mt_warning_utf8.txt'), 'rbU') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        data = f.read()
        analyser = BiGramAnalyser(bi_grams, stopword_list=stopwords.ENGLISH_TEST)
        index = Index.create(schema.Schema(text=schema.TEXT(analyser=analyser)))
        index.add_document(text=data, frame_size=2)
        searcher = index.searcher()

        assert searcher.count(QSQ('1770')) == 2
        assert searcher.count(QSQ('1,900')) == 1
        assert searcher.count(QSQ('4.4')) == 1
        assert searcher.count(QSQ('*')) == index.get_frame_count()


def test_searching_twitter():
    """Test searching twitter data."""
    with open('caterpillar/test_resources/twitter_sentiment.csv', 'rbU') as f:
        analyser = DefaultAnalyser(stopword_list=stopwords.ENGLISH_TEST)
        index = Index.create(schema.Schema(text=schema.TEXT(analyser=analyser),
                                           sentiment=schema.CATEGORICAL_TEXT(indexed=True)))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(text=row[1], sentiment=row[0])
        index.reindex()
        searcher = index.searcher()

        assert searcher.count(QSQ('@NYSenate')) == 1
        assert searcher.count(QSQ('summerdays@gmail.com')) == 1
        assert searcher.count(QSQ('sentiment=positive')) + \
            searcher.count(QSQ('sentiment=negative')) == index.get_frame_count()


def test_searching_nps():
    """Test searching nps-backed data."""
    with open('caterpillar/test_resources/big.csv', 'rbU') as f:
        analyser = DefaultAnalyser(stopword_list=stopwords.ENGLISH_TEST)
        index = Index.create(schema.Schema(respondant=schema.NUMERIC,
                                           region=schema.CATEGORICAL_TEXT(indexed=True),
                                           store=schema.CATEGORICAL_TEXT(indexed=True),
                                           liked=schema.TEXT(analyser=analyser),
                                           disliked=schema.TEXT(analyser=analyser),
                                           would_like=schema.TEXT(analyser=analyser),
                                           nps=schema.NUMERIC(indexed=True),
                                           fake=schema.NUMERIC(indexed=True),
                                           fake2=schema.CATEGORICAL_TEXT(indexed=True)))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        empty_rows = 0
        for row in csv_reader:
            if len(row[3]) + len(row[4]) + len(row[5]) == 0:
                empty_rows += 1
            index.add_document(update_index=False, respondant=row[0], region=row[1], store=row[2], liked=row[3],
                               disliked=row[4], would_like=row[5], nps=row[6], fake2=None)

        index.reindex()
        searcher = index.searcher()

        # Search limited by text field
        assert index.get_frame_count() == empty_rows + searcher.count(QSQ('*', 'disliked'))\
            + searcher.count(QSQ('*', 'liked')) + searcher.count(QSQ('*', 'would_like'))
        assert searcher.count(QSQ('point*', 'would_like'))\
            == searcher.count(QSQ('point or points or pointed', 'would_like'))

        # Verify uniqueness of returned results
        docs = set()
        results = searcher.search(QSQ('region=Otago and nps<5'))
        for hit in results:
            docs.add(hit.doc_id)
        assert len(docs) == 5
        assert len(results) == 15

        # Metadata field searching
        assert searcher.count(QSQ('nps=10 and store=DANNEVIRKE')) == 6
        num_christchurch = searcher.count(QSQ('region=Christchurch'))
        num_null_nps_christchurch = num_christchurch \
            - searcher.count(QSQ('region=Christchurch and nps > 0'))
        assert num_christchurch == searcher.count(QSQ('region=Christchurch and nps < 8')) \
            + searcher.count(QSQ('region=Christchurch and nps >= 8')) \
            + num_null_nps_christchurch
        assert searcher.count(QSQ('region=Christchurch and nps>7 and (reliable or quick)')) \
            == searcher.count(QSQ('region = Christchurch and nps>7')) \
            - searcher.count(QSQ('region=Christchurch and nps > 7 not (reliable or quick)'))
        assert searcher.count(QSQ('nps>0')) == searcher.count(QSQ('nps<=7')) \
            + searcher.count(QSQ('nps>7'))
        assert searcher.count(QSQ('region=Christ*')) == num_christchurch == 1399
        assert searcher.count(QSQ('fake=1')) == 0
        assert searcher.count(QSQ('fake2=something')) == 0
        assert searcher.count(QSQ('region=nonexistentregion')) == 0

        # Check all incorrect usages of metadata field searching
        with pytest.raises(QueryError):
            searcher.count(QSQ('nps >= 1?'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('nps=?'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('n*s=10'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('badfield=something'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('liked=something'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('region>something'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('region>=something'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('region<something'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('region<=something'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('respondant=something'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('region>Christchurch'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('nps>bad'))
        with pytest.raises(QueryError):
            searcher.count(QSQ('*', 'badfield'))


def test_searching_nps_no_text_update():
    """Test retrieving by ID with no text."""
    with open('caterpillar/test_resources/big.csv', 'rbU') as f:
        index = Index.create(schema.Schema(respondant=schema.ID(indexed=True),
                                           region=schema.CATEGORICAL_TEXT(indexed=True), nps=schema.NUMERIC))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(update_index=False, respondant=row[0], region=row[1], nps=row[6])
        index.reindex(update_only=True)
        searcher = index.searcher()
        assert searcher.count(QSQ('respondant = 1')) == 1
        assert searcher.count(QSQ('region = Chr*')) == 878


def test_no_data_by_circumstance():
    """Test that when we add data with no indexed fields we can't retrieve it."""
    with open('caterpillar/test_resources/test_small.csv', 'rbU') as f:
        index = Index.create(schema.Schema(respondant=schema.ID, nps=schema.NUMERIC))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        for row in csv_reader:
            index.add_document(update_index=False, respondant=row[0], region=row[1], nps=row[6])
        index.reindex(update_only=True)
        searcher = index.searcher()
        assert searcher.count(QSQ('*')) == 0


def test_searching_reserved_words():
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        index = Index.create(schema.Schema(text=schema.TEXT(analyser=DefaultAnalyser(stopword_list=[]))))
        index.add_document(text=data, frame_size=2)
        index.fold_term_case()
        searcher = index.searcher()

        assert searcher.count(QSQ('"and"')) == len(index.get_term_positions('and')) == 474
        assert searcher.count(QSQ('"or"')) == 0
        assert searcher.count(QSQ('"not"')) == 117

        with pytest.raises(QueryError):
            # Unescaped reserved word
            searcher.count(QSQ('and'))
