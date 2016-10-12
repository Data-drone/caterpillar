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
            writer.fold_term_case('text')

        # Merge bigrams
        with IndexReader(index_dir) as reader:
            bigrams = find_bi_gram_words(reader.get_frames('text'))
        with IndexWriter(index_dir) as writer:
            writer.merge_terms(merges=[((bigram.split(' ')[0], bigram.split(' ')[1]), bigram) for bigram in bigrams],
                               text_field='text')

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ("King", 'text')) == searcher.count(QSQ("K?ng", 'text'))
            assert searcher.count(QSQ("Queen or K??g", 'text')) == 123 == \
                searcher.count(QSQ("King or Queen", 'text'))
            assert searcher.count(QSQ("King AND Queen", 'text')) == 4 == \
                searcher.count(MatchAllQuery([QSQ('King', 'text'), QSQ('Queen', 'text')])) == \
                searcher.count(QSQ('King', 'text')) - searcher.count(QSQ('King not Queen', 'text'))
            assert searcher.count(QSQ("King NOT Queen", 'text')) == 56
            assert searcher.count(QSQ('golden key', 'text')) == 6
            assert searcher.count(QSQ('*ing', 'text')) == 512
            assert searcher.count(QSQ("Alice and (thought or little)", 'text')) == \
                searcher.count(QSQ("Alice and thought or Alice and little", 'text')) == 95 == \
                searcher.count(MatchAllQuery([QSQ('Alice', 'text'), MatchSomeQuery([QSQ('thought', 'text'), QSQ('little', 'text')])]))
            assert searcher.count(QSQ("thistermdoesntexist", 'text')) == 0
            assert searcher.count(QSQ('Mock Turtle', 'text')) == 51
            assert searcher.count(QSQ('*t? R*b??', 'text')) == searcher.count(QSQ('White Rabbit', 'text'))

            # Test that filtering, counts and searching all return the same number of results.
            assert searcher.count(QSQ("King", 'text')) == \
                   len(searcher.filter(QSQ("King", 'text'))) == \
                   searcher.search(QSQ("King", 'text')).num_matches

            assert "jury" in searcher.search(QSQ("jury", 'text'), limit=1)[0].data['text']

            voice_hits = searcher.count(QSQ("voice", 'text'))
            assert voice_hits == 46
            misses = 0
            results = searcher.search(QSQ("Alice or voice", 'text'), limit=voice_hits)
            for hit in results:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 0
            misses = 0
            results = searcher.search(QSQ("Alice or voice^0.2", 'text'), limit=voice_hits)
            for hit in results:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 30
            misses = 0
            results = searcher.search(QSQ("Alice or voice^0.5", 'text'), limit=voice_hits)
            for hit in results:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 15
            results = searcher.search(QSQ("Alice or voice^20", 'text'), limit=voice_hits)
            for hit in results:
                assert "voice" in hit.tfs
            misses = 0
            results = searcher.search(QSQ("Alice or voice", 'text'), limit=0)
            for hit in results[-voice_hits:]:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == voice_hits
            misses = 0
            results = searcher.search(QSQ("Alice^20 or voice", 'text'), limit=0)
            for hit in results[-voice_hits:]:
                misses = misses + (1 if "voice" not in hit.tfs else 0)
            assert misses == 16

            results = searcher.search(QSQ("King not (court or evidence)", 'text'))
            assert len(results) == 25
            assert len(results.term_weights) == 1
            assert results.num_matches == 53 == searcher.count(MatchAllQuery([QSQ('King', 'text')], 
                                                               [QSQ('court or evidence', 'text')]))
            for hit in results:
                assert "evidence" not in hit.data['text']
                assert "court" not in hit.data['text']
                assert hit.data['_field'] == 'text'
                for k in results[0].data.iterkeys():
                    assert k in ('_id', '_doc_id', '_field') or not k.startswith('_')

            # Check multiple boostings; this example is totally contrived but a real case could occur when combining
            # different plugin queries. Also ensure that the term boosting is independent of query order.
            results1 = searcher.search(MatchSomeQuery([QSQ("King", 'text'), QSQ("court AND King^1.5", 'text')]))
            results2 = searcher.search(MatchSomeQuery([QSQ("court AND King^1.5", 'text'), QSQ("King", 'text')]))
            assert results1.term_weights['text']['King'] == 1.5
            assert results2.term_weights['text']['King'] == 1.5

            with pytest.raises(TypeError):
                # Invalid query format
                searcher.count(['hello', 'text'])


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
            bigrams = find_bi_gram_words(reader.get_frames('text'))
        with IndexWriter(index_dir) as writer:
            writer.merge_terms(merges=[((bigram.split(' ')[0], bigram.split(' ')[1]), bigram) for bigram in bigrams],
                               text_field='text')

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher(scorer_cls=TfidfScorer)
            results = searcher.search(QSQ('Alice or Caterpillar', 'text'))
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
            bigrams = find_bi_gram_words(reader.get_frames('text'))
        with IndexWriter(index_dir) as writer:
            writer.merge_terms(merges=[((bigram.split(' ')[0], bigram.split(' ')[1]), bigram) for bigram in bigrams],
                               text_field='text')

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('1770', 'text')) == 2
            assert searcher.count(QSQ('1,900', 'text')) == 1
            assert searcher.count(QSQ('4.4', 'text')) == 1
            assert searcher.count(QSQ('*', 'text')) == reader.get_frame_count('text')


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
            assert searcher.count(QSQ('@NYSenate', 'text')) == 1
            assert searcher.count(QSQ('summerdays@gmail.com', 'text')) == 1
            assert searcher.count(QSQ('sentiment=positive', 'text')) + \
                   searcher.count(QSQ('sentiment=negative', 'text')) == reader.get_frame_count('text')


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
                writer.add_document(respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                    disliked=row[4], would_like=row[5], nps=row[6], fake2=None)

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('point*', 'would_like'))\
                == searcher.count(QSQ('point or points or pointed', 'would_like'))

            # Verify uniqueness of returned results
            docs = set()
            result_count = 0
            for field in ['liked', 'disliked', 'would_like']:
                results = searcher.search(QSQ('region=Otago and nps<5', field))
                result_count += len(results)
                for hit in results:
                    docs.add(hit.doc_id)
            assert len(docs) == 5
            assert result_count == 15

            # Metadata field searching
            assert searcher.count(QSQ('nps=10 and store=DANNEVIRKE', 'liked')) + \
                   searcher.count(QSQ('nps=10 and store=DANNEVIRKE', 'disliked')) + \
                   searcher.count(QSQ('nps=10 and store=DANNEVIRKE', 'would_like')) == 6

            num_christchurch = searcher.count(QSQ('region=Christchurch', 'liked'))

            num_null_nps_christchurch = num_christchurch - searcher.count(QSQ('region=Christchurch and nps > 0', 'liked'))

            assert num_christchurch == searcher.count(QSQ('region=Christchurch and nps < 8', 'liked')) + \
                                       searcher.count(QSQ('region=Christchurch and nps >= 8', 'liked')) + \
                                       num_null_nps_christchurch

            assert searcher.count(QSQ('region=Christchurch and nps>7 and (reliable or quick)', 'disliked')) \
                == searcher.count(QSQ('region = Christchurch and nps>7', 'disliked')) \
                - searcher.count(QSQ('region=Christchurch and nps > 7 not (reliable or quick)', 'disliked'))
            assert searcher.count(QSQ('nps>0', 'would_like')) == searcher.count(QSQ('nps<=7', 'would_like')) \
                + searcher.count(QSQ('nps>7', 'would_like'))
            assert searcher.count(QSQ('region=Christ*', 'liked')) == num_christchurch
            assert searcher.count(QSQ('fake=1', 'liked')) == 0
            assert searcher.count(QSQ('fake2=something', 'liked')) == 0
            assert searcher.count(QSQ('region=nonexistentregion', 'liked')) == 0

            # Check all incorrect usages of metadata field searching
            with pytest.raises(QueryError):
                searcher.count(QSQ('nps >= 1?', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('nps=?', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('n*s=10', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('badfield=something', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('liked=something', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('region>something', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('region>=something', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('region<something', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('region<=something', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('respondant=something', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('region>Christchurch', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('nps>bad', 'liked'))
            with pytest.raises(QueryError):
                searcher.count(QSQ('*', 'badfield'))


def test_searching_reserved_words(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(
            text=schema.TEXT(analyser=TestAnalyser(stopword_list=[]))))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)
            writer.fold_term_case('text')

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            assert searcher.count(QSQ('"and"', 'text')) == sum(1 for _ in reader.get_term_positions('and', 'text')) == 469
            assert searcher.count(QSQ('"or"', 'text')) == 0
            assert searcher.count(QSQ('"not"', 'text')) == 117

            with pytest.raises(QueryError):
                # Unescaped reserved word
                searcher.count(QSQ('and', 'text'))
