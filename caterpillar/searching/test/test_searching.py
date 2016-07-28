# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""Tests for the searching package."""
import csv
import os

import pytest

from caterpillar.processing.index import find_bi_gram_words, IndexReader, IndexWriter, IndexConfig
from caterpillar.processing import schema
from caterpillar.searching.query import QueryError
from caterpillar.searching.query.match import MatchAllQuery, MatchSomeQuery
from caterpillar.searching.query.querystring import QueryStringQuery as QSQ
from caterpillar.searching.scoring import TfidfScorer
from caterpillar.storage.sqlite import SqliteStorage
from caterpillar.test_util import TestAnalyser


def test_searching_alice(index_dir):
    """Test basic searching functions for Alice."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        f.seek(0)
        data = f.read()
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT(analyser=analyser)))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)
            writer.fold_term_case()

        # Merge bigrams
        with IndexReader(index_dir) as reader:
            bigrams = find_bi_gram_words(reader.get_frames())
        with IndexWriter(index_dir) as writer:
            writer.merge_terms(merges=[((bigram.split(' ')[0], bigram.split(' ')[1]), bigram) for bigram in bigrams])

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ("King")) == searcher.count(QSQ("K?ng"))
            assert searcher.count(QSQ("Queen or K??g")) == 123 == \
                searcher.count(QSQ("King or Queen"))
            assert searcher.count(QSQ("King AND Queen")) == 4 == \
                searcher.count(MatchAllQuery([QSQ('King'), QSQ('Queen')])) == \
                searcher.count(QSQ('King')) - searcher.count(QSQ('King not Queen'))
            assert searcher.count(QSQ("King NOT Queen")) == 56
            assert searcher.count(QSQ('golden key')) == 6
            assert searcher.count(QSQ('*ing')) == 512
            assert searcher.count(QSQ("Alice and (thought or little)")) == \
                searcher.count(QSQ("Alice and thought or Alice and little")) == 95 == \
                searcher.count(MatchAllQuery([QSQ('Alice'), MatchSomeQuery([QSQ('thought'), QSQ('little')])]))
            assert searcher.count(QSQ("thistermdoesntexist")) == 0
            assert searcher.count(QSQ('Mock Turtle')) == 51
            assert searcher.count(QSQ('*t? R*b??')) == searcher.count(QSQ('White Rabbit'))

            assert "jury" in searcher.search(QSQ("jury"), limit=1)[0].data['text']

            voice_hits = searcher.count(QSQ("voice"))
            assert voice_hits == 46
            misses = 0
            results = searcher.search(QSQ("Alice or voice"), limit=voice_hits)
            for hit in results:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 0
            misses = 0
            results = searcher.search(QSQ("Alice or voice^0.2"), limit=voice_hits)
            for hit in results:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 30
            misses = 0
            results = searcher.search(QSQ("Alice or voice^0.5"), limit=voice_hits)
            for hit in results:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 15
            results = searcher.search(QSQ("Alice or voice^20"), limit=voice_hits)
            for hit in results:
                assert "voice" in hit.tfs
            misses = 0
            results = searcher.search(QSQ("Alice or voice"), limit=0)
            for hit in results[-voice_hits:]:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == voice_hits
            misses = 0
            results = searcher.search(QSQ("Alice^20 or voice"), limit=0)
            for hit in results[-voice_hits:]:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 16

            results = searcher.search(QSQ("King not (court or evidence)"))
            assert len(results) == 25
            assert len(results.term_weights) == 1
            assert results.num_matches == 53 == searcher.count(MatchAllQuery([QSQ('King')], [QSQ('court or evidence')]))
            for hit in results:
                assert "evidence" not in hit.data['text']
                assert "court" not in hit.data['text']
                assert hit.data['_field'] == 'text'
                for k in results[0].data.iterkeys():
                    assert k in ('_id', '_doc_id', '_field') or not k.startswith('_')

            # Check multiple boostings; this example is totally contrived but a real case could occur when combining
            # different plugin queries.
            results = searcher.search(MatchSomeQuery([QSQ("King"), QSQ("court AND King^1.5")]))
            assert results.term_weights['King'] == 1.5

            with pytest.raises(TypeError):
                # Invalid query format
                searcher.count('hello')


def test_searching_alice_simple(index_dir):
    """Test searching for Alice with the simple scorer."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        f.seek(0)
        data = f.read()
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT(analyser=analyser)))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)

        # Merge bigrams
        with IndexReader(index_dir) as reader:
            bigrams = find_bi_gram_words(reader.get_frames())
        with IndexWriter(index_dir) as writer:
            writer.merge_terms(merges=[((bigram.split(' ')[0], bigram.split(' ')[1]), bigram) for bigram in bigrams])

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher(scorer_cls=TfidfScorer)
            results = searcher.search(QSQ('Alice or Caterpillar'))
            text = results[0].data[results[0].text_field]
            assert len(results) == 25
            assert 'Alice' in text and 'Caterpillar' in text
            assert 'Alice' not in results[-1].data[results[-1].text_field]


def test_searching_mt_warning(index_dir):
    """Test searching for mt warning data."""
    with open(os.path.abspath('caterpillar/test_resources/mt_warning_utf8.txt'), 'rbU') as f:
        data = f.read()
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT(analyser=analyser)))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)

        # Merge bigrams
        with IndexReader(index_dir) as reader:
            bigrams = find_bi_gram_words(reader.get_frames())
        with IndexWriter(index_dir) as writer:
            writer.merge_terms(merges=[((bigram.split(' ')[0], bigram.split(' ')[1]), bigram) for bigram in bigrams])

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('1770')) == 2
            assert searcher.count(QSQ('1,900')) == 1
            assert searcher.count(QSQ('4.4')) == 1
            assert searcher.count(QSQ('*')) == reader.get_frame_count()


def test_searching_twitter(index_dir):
    """Test searching twitter data."""
    with open('caterpillar/test_resources/twitter_sentiment.csv', 'rbU') as f:
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT(analyser=analyser),
                                                                 sentiment=schema.CATEGORICAL_TEXT(indexed=True)))
        with IndexWriter(index_dir, config) as writer:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            for row in csv_reader:
                writer.add_document(text=row[1], sentiment=row[0])

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('@NYSenate')) == 1
            assert searcher.count(QSQ('summerdays@gmail.com')) == 1
            assert searcher.count(QSQ('sentiment=positive')) + \
                searcher.count(QSQ('sentiment=negative')) == reader.get_frame_count()


def test_searching_nps(index_dir):
    """Test searching nps-backed data."""
    with open('caterpillar/test_resources/big.csv', 'rbU') as f:
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema.Schema(respondant=schema.NUMERIC,
                                                          region=schema.CATEGORICAL_TEXT(indexed=True),
                                                          store=schema.CATEGORICAL_TEXT(indexed=True),
                                                          liked=schema.TEXT(analyser=analyser),
                                                          disliked=schema.TEXT(analyser=analyser),
                                                          would_like=schema.TEXT(analyser=analyser),
                                                          nps=schema.NUMERIC(indexed=True),
                                                          fake=schema.NUMERIC(indexed=True),
                                                          fake2=schema.CATEGORICAL_TEXT(indexed=True)))
        with IndexWriter(index_dir, config) as writer:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            empty_rows = 0
            for row in csv_reader:
                if len(row[3]) + len(row[4]) + len(row[5]) == 0:
                    empty_rows += 1
                writer.add_document(update_index=False, respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                    disliked=row[4], would_like=row[5], nps=row[6], fake2=None)

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            # Search limited by text field
            assert reader.get_frame_count() == empty_rows + searcher.count(QSQ('*', 'disliked'))\
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


def test_searching_nps_no_text(index_dir):
    """Test retrieving by ID with no text."""
    with open('caterpillar/test_resources/big.csv', 'rbU') as f:
        config = IndexConfig(SqliteStorage, schema.Schema(respondant=schema.ID(indexed=True),
                                                          region=schema.CATEGORICAL_TEXT(indexed=True),
                                                          nps=schema.NUMERIC))
        with IndexWriter(index_dir, config) as writer:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            for row in csv_reader:
                writer.add_document(update_index=False, respondant=row[0], region=row[1], nps=row[6])

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('respondant = 1')) == 1
            assert searcher.count(QSQ('region = Chr*')) == 878
            assert searcher.filter(QSQ('respondant = 9846'))


def test_no_data_by_circumstance(index_dir):
    """Test that when we add data with no indexed fields we can't retrieve it."""
    with open('caterpillar/test_resources/test_small.csv', 'rbU') as f:
        config = IndexConfig(SqliteStorage, schema=schema.Schema(respondant=schema.ID, nps=schema.NUMERIC))
        with IndexWriter(index_dir, config) as writer:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            for row in csv_reader:
                writer.add_document(update_index=False, respondant=row[0], region=row[1], nps=row[6])

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('*')) == 0


def test_searching_reserved_words(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(
            text=schema.TEXT(analyser=TestAnalyser(stopword_list=[]))))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)
            writer.fold_term_case()

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('"and"')) == sum(1 for _ in reader.get_term_positions('and')) == 469
            assert searcher.count(QSQ('"or"')) == 0
            assert searcher.count(QSQ('"not"')) == 117

            with pytest.raises(QueryError):
                # Unescaped reserved word
                searcher.count(QSQ('and'))
