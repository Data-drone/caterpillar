# Copyright (c) 2012-2014 Kapiche Limited
# Author: Sam Hames <sam.hames@kapiche.com>
"""Test search functionality:

1. Low level filter and filter_and_rank search primitives.
2. Integration of the resultset functionality with search output.

"""
from __future__ import division

import csv
import os
import pytest

from caterpillar import composition
from caterpillar.storage.sqlite import SqliteStorage
from caterpillar.processing.index import IndexWriter, IndexReader, IndexConfig
from caterpillar.processing import schema
from caterpillar.test_util import TestAnalyser


def test_searching_filtering_nps(index_dir):
    """Test searching nps-backed data."""
    with open('caterpillar/test_resources/big.csv', 'rbU') as f:
        analyser = TestAnalyser()
        config = IndexConfig(
            SqliteStorage, schema.Schema(
                respondant=schema.NUMERIC,
                region=schema.CATEGORICAL_TEXT(indexed=True),
                store=schema.CATEGORICAL_TEXT(indexed=True),
                liked=schema.TEXT(analyser=analyser),
                disliked=schema.TEXT(analyser=analyser),
                would_like=schema.TEXT(analyser=analyser),
                nps=schema.NUMERIC(indexed=True),
                fake=schema.NUMERIC(indexed=True),
                fake2=schema.CATEGORICAL_TEXT(indexed=True),
                fake3=schema.CATEGORICAL_TEXT(indexed=True)
            )
        )
        with IndexWriter(index_dir, config) as writer:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            empty_rows = 0
            for row in csv_reader:
                if len(row[3]) + len(row[4]) + len(row[5]) == 0:
                    empty_rows += 1
                writer.add_document(respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                    disliked=row[4], would_like=row[5], nps=row[6], fake2=None, fake3=' spaces ')

        with IndexReader(index_dir) as reader:

            results = reader.filter(should=['point', 'pointed', 'points'], include_fields=['would_like'])
            assert len(results) == 14

            # Verify uniqueness of returned results
            docs = set()
            result_count = 0
            for field in ['liked', 'disliked', 'would_like']:
                # query returns the same 5 documents for each unstructured field searched.
                results = reader.filter(metadata={'region': {'=': 'Otago'}, 'nps': {'<': 5}}, include_fields=[field])
                result_count += len(results)
                for frame_id, frame in reader.get_frames(frame_ids=results, field=None):
                    docs.add(frame['_doc_id'])

            # Individual field searches should match the overall field searches.
            assert len(docs) == 5 == len(
                reader.filter(
                    metadata={'region': {'=': 'Otago'}, 'nps': {'<': 5}}, return_documents=True
                )
            )
            assert result_count == 15 == len(
                reader.filter(
                    metadata={'region': {'=': 'Otago'}, 'nps': {'<': 5}}
                )
            )

            # Metadata field searching
            metadata = {
                'nps': {'=': 10},
                'store': {'=': 'DANNEVIRKE'}
            }
            assert len(reader.filter(metadata=metadata)) == 6 == \
                len(reader.filter(metadata=metadata, include_fields=['liked'])) + \
                len(reader.filter(metadata=metadata, include_fields=['disliked'])) + \
                len(reader.filter(metadata=metadata, include_fields=['would_like']))

            num_christchurch = len(reader.filter(metadata={'region': {'=': 'Christchurch'}}, include_fields=['liked']))
            valid_nps_christchurch = len(
                reader.filter(metadata={'region': {'=': 'Christchurch'}, 'nps': {'>': 0}}, include_fields=['liked'])
            )
            assert valid_nps_christchurch < num_christchurch

            # Test invariant properties of sets for comparing a subset of terms.
            all_terms = {term: freq for i, (term, freq) in enumerate(reader.get_frequencies('liked')) if i % 40 == 0}

            for left_term in all_terms:
                for right_term in all_terms:

                    if left_term == right_term:
                        continue

                    left_or_right = len(reader.filter(should=[left_term, right_term], include_fields=['liked']))
                    left_and_right = len(reader.filter(must=[left_term, right_term], include_fields=['liked']))
                    left_not_right = len(reader.filter(
                        should=[left_term], must_not=[right_term], include_fields=['liked'])
                    )
                    right_not_left = len(reader.filter(
                        must_not=[left_term], should=[right_term], include_fields=['liked'])
                    )

                    assert left_or_right == (left_and_right + left_not_right + right_not_left)
                    assert left_and_right <= min(all_terms[left_term], all_terms[right_term])

            for i in range(1, 11):
                total = len(reader.filter(metadata={'nps': {'>': 0}}))
                assert (
                    len(reader.filter(metadata={'nps': {'>=': i}})) +
                    len(reader.filter(metadata={'nps': {'<': i}})) ==
                    total
                )

            assert (
                len(reader.filter(metadata={'nps': {'>=': 1, '<=': 3}})) ==
                len(reader.filter(metadata={'nps': {'in': (1, 2, 3)}}))
            )
            assert (
                len(reader.filter(metadata={'nps': {'>=': 1, '<=': 3}}, must=['point'])) ==
                len(reader.filter(metadata={'nps': {'in': (1, 2, 3)}}, must=['point']))
            )
            assert len(reader.filter(metadata={'fake': {'=': 1}})) == 0
            assert len(reader.filter(metadata={'region': {'=': 'asdfjhsdfsdfa'}})) == 0
            assert (
                len(reader.filter(metadata={'fake3': {'=': ' spaces '}}, include_fields=['liked'])) ==
                reader.get_frame_count('liked')
            )

            # empty resulset - filtering and ranking
            assert len(reader.filter_and_rank(must=['doesnotexist'])) == 0
            assert len(reader.filter_and_rank(must=['doesnotexist'], return_documents=True)) == 0

            # Pagination and limit options for filter
            # 1. Documents and only metadata
            documents = reader.filter(metadata={'fake3': {'=': ' spaces '}}, return_documents=True, limit=5)
            assert len(documents) == 5
            next_page = reader.filter(
                metadata={'fake3': {'=': ' spaces '}}, return_documents=True, limit=5, pagination_key=max(documents)
            )
            assert max(next_page) == max(documents) + 5  # True iff there are no deletes

            # 2. Documents, metadata and text
            documents = reader.filter(
                metadata={'fake3': {'=': ' spaces '}}, must=['point'], return_documents=True, limit=5
            )
            assert len(documents) == 5
            next_page = reader.filter(
                metadata={'fake3': {'=': ' spaces '}}, must=['point'], return_documents=True, limit=5,
                pagination_key=max(documents)
            )
            assert [i > max(documents) for i in next_page.values()]

            # 3. Frames and only metadata
            frames = reader.filter(metadata={'fake3': {'=': ' spaces '}}, limit=5)
            assert len(frames) == 5
            next_page = reader.filter(
                metadata={'fake3': {'=': ' spaces '}}, limit=5, pagination_key=max(frames)
            )
            assert max(next_page) == max(frames) + 5  # True iff there are no deletes

            # 4. Frames, metadata and text
            frames = reader.filter(
                metadata={'fake3': {'=': ' spaces '}}, must=['point'], limit=5
            )
            assert len(frames) == 5
            next_page = reader.filter(
                metadata={'fake3': {'=': ' spaces '}}, must=['point'], limit=5,
                pagination_key=max(frames)
            )
            assert [i > max(frames) for i in next_page.values()]

            # Compare the two paths for scoring for frames including pagination
            frames = reader.filter(metadata={'fake3': {'=': ' spaces '}}, should=['fly', 'buys', 'points'])
            scored_frames = composition.score_and_rank(frames, limit=10)
            searched_frames = reader.filter_and_rank(
                metadata={'fake3': {'=': ' spaces '}}, should=['fly', 'buys', 'points'], limit=10
            )
            paged_search_frames = reader.filter_and_rank(
                metadata={'fake3': {'=': ' spaces '}}, should=['fly', 'buys', 'points'], limit=5,
                pagination_key=scored_frames[4]
            )
            for i, j in zip(scored_frames, searched_frames):
                # Same frame_ids, OR, same score with different frame id
                assert i[0] == j[0]

            for i, j in zip(scored_frames[5:], paged_search_frames):
                # Same frame_ids, OR, same score with different frame id
                assert i[0] == j[0]

            # Compare the two paths for scoring for documents including pagination
            documents = reader.filter(
                metadata={'fake3': {'=': ' spaces '}}, return_documents=True,
                should=['fly', 'buys', 'points']
            )
            scored_documents = composition.score_and_rank(documents, limit=10)
            searched_documents = reader.filter_and_rank(
                metadata={'fake3': {'=': ' spaces '}}, return_documents=True,
                should=['fly', 'buys', 'points'], limit=10
            )
            paged_search_documents = reader.filter_and_rank(
                metadata={'fake3': {'=': ' spaces '}}, return_documents=True,
                should=['fly', 'buys', 'points'], limit=5,
                pagination_key=scored_documents[4]
            )
            for i, j in zip(scored_documents, searched_documents):
                # Same frame_ids, OR, same score with different frame id
                assert i[0] == j[0]

            for i, j in zip(scored_documents[5:], paged_search_documents):
                # Same frame_ids, OR, same score with different frame id
                assert i[0] == j[0]

            # Check incorrect usage of various search things.
            with pytest.raises(ValueError):  # Unknown operator
                reader.filter(metadata={'fake3': {'x': ' spaces '}})
            with pytest.raises(ValueError):  # Invalid operator for a valid field
                reader.filter(metadata={'fake3': {'>': ' spaces '}})
            with pytest.raises(ValueError):  # Unknown field
                reader.filter(metadata={'fake5': {'>': ' spaces '}})
            with pytest.raises(ValueError):  # Valid field, but not supported by SQLite
                reader.filter(metadata={'fake3': {'*=': ' spaces '}})
            with pytest.raises(ValueError):  # Valid field, but not supported by SQLite
                reader.filter(metadata={'fake3': {'*=': ' spaces '}}, must=['point'])
            with pytest.raises(ValueError):  # Must_not without driving terms raises an error
                reader.filter(must_not=['potato'])
            with pytest.raises(ValueError):  # Ranking not supported for metadata only
                reader.filter_and_rank(metadata={'fake3': {'=': ' spaces '}})


def test_reader_query_basic(index_dir):
    """Test querystring query basic functionality."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text1=schema.TEXT, text2=schema.TEXT))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text1=data, text2=data)

    # Simple terms
    with IndexReader(index_dir) as reader:
        alice_count = len(reader.filter(must=['Alice'], include_fields=['text1']))
        king_count = len(reader.filter(must=['King'], include_fields=['text1']))
        assert alice_count > 0
        assert king_count > 0

        # Boolean operators
        alice_and_king_count = len(reader.filter(must=['King', 'Alice'], include_fields=['text1']))
        alice_and_king_all_fields = len(reader.filter(must=['King', 'Alice']))
        alice_or_king_count = len(reader.filter(should=['King', 'Alice'], include_fields=['text1']))
        alice_not_king_count = len(reader.filter(must_not=['King'], must=['Alice'], include_fields=['text1']))
        king_not_alice_count = len(reader.filter(must_not=['Alice'], must=['King'], include_fields=['text1']))
        not_king_not_alice_count = len(composition.exclude(
            {frame_id: [0] for frame_id in reader.get_frame_ids('text1')},
            reader.filter(should=['King', 'Alice'], include_fields=['text1'])
        ))

        assert alice_not_king_count == alice_count - alice_and_king_count
        assert king_not_alice_count == king_count - alice_and_king_count
        assert alice_or_king_count == alice_not_king_count + king_not_alice_count + alice_and_king_count
        assert not_king_not_alice_count == reader.get_frame_count('text1') - alice_or_king_count

        at_least_2 = len(reader.filter(at_least_n=(2, ['King', 'Alice']), include_fields=['text1']))
        at_least_2_ex = len(reader.filter(at_least_n=(2, ['King', 'Alice', 'doesnotexist']), include_fields=['text1']))
        at_least_1 = len(reader.filter(at_least_n=(1, ['King', 'Alice']), include_fields=['text1']))
        at_least_4 = len(reader.filter(at_least_n=(4, ['King', 'Alice']), include_fields=['text1']))

        assert at_least_2 == at_least_2_ex == alice_and_king_count
        assert at_least_1 == alice_or_king_count
        assert at_least_4 == 0

        # Wildcards - not currently supported
        # assert len(reader.filter(should=['*ice'], include_fields=['text1'])) > alice_count
        # assert len(reader.filter(should=['K??g'], include_fields=['text1'])) == king_count

        # Raises an error if the field doesn't exist
        with pytest.raises(ValueError):
            alice_and_king_count = len(reader.filter(must=['King', 'Alice'], include_fields=['doesnotexist']))

        # Boolean Operators across fields
        field1 = reader.filter(must=['King', 'Alice'], include_fields=['text1'])
        field2 = reader.filter(must=['King', 'Alice'], include_fields=['text2'])

        and_fields = composition.match_all(field1, field2)
        or_fields = composition.match_any(field1, field2)

        assert len(and_fields) == 0
        assert len(or_fields) == alice_and_king_all_fields

        # Now test some document retrieval:
        alice_document_count = len(reader.filter(must=['Alice'], include_fields=['text1'], return_documents=True))
        king_document_count = len(reader.filter(must=['King'], include_fields=['text1'], return_documents=True))
        assert alice_document_count == king_document_count == 1

    with IndexWriter(index_dir, config) as writer:
        writer.add_document(text1='a Alice', text2='b King')

    # Simple terms
    with IndexReader(index_dir) as reader:
        alice_document_count1 = len(reader.filter(must=['Alice'], include_fields=['text1'], return_documents=True))
        king_document_count1 = len(reader.filter(must=['King'], include_fields=['text1'], return_documents=True))
        alice_document_count2 = len(reader.filter(must=['Alice'], include_fields=['text2'], return_documents=True))
        king_document_count2 = len(reader.filter(must=['King'], include_fields=['text2'], return_documents=True))
        alice_document_count_all = len(reader.filter(must=['Alice'], return_documents=True))
        king_document_count_all = len(reader.filter(must=['King'], return_documents=True))
        assert alice_document_count1 == king_document_count2 == 2
        assert alice_document_count2 == king_document_count1 == 1
        assert alice_document_count_all == king_document_count_all == 2


def test_reader_query_advanced(index_dir):
    """Test querysting query advanced searching."""
    config = IndexConfig(SqliteStorage, schema.Schema(liked=schema.TEXT, disliked=schema.TEXT,
                                                      age=schema.NUMERIC(indexed=True),
                                                      gender=schema.CATEGORICAL_TEXT(indexed=True)))
    with IndexWriter(index_dir, config) as writer:
        writer.add_document(liked='product', disliked='service', age=20, gender='male')
        writer.add_document(liked='service', disliked='product', age=30, gender='male')
        writer.add_document(liked='service', disliked='price', age=40, gender='female')
        writer.add_document(liked='product', disliked='product', age=80, gender='female')

    # Metadata
    with IndexReader(index_dir) as reader:
        # Test presence of terms for each text_field
        # field: "liked":
        assert len(reader.filter(metadata={'age': {'=': 80}}, include_fields=['liked'])) == 1
        assert len(reader.filter(metadata={'age': {'<': 80}}, include_fields=['liked'])) == 3
        assert len(reader.filter(metadata={'age': {'>=': 20}}, include_fields=['liked'])) == 4
        assert len(reader.filter(must=['product'], metadata={'gender': {'=': 'female'}}, include_fields=['liked'])) == 1

        product_results = reader.filter(must=['product'], include_fields=['liked'])
        male_gender = reader.filter(metadata={'gender': {'=': 'male'}}, include_fields=['liked'])
        both_genders = reader.filter(metadata={'gender': {'in': ('female', 'male')}}, include_fields=['liked'])

        assert len(composition.exclude(product_results, male_gender)) == 1
        assert len(composition.exclude(product_results, both_genders)) == 0

        # field: "disliked":
        assert len(reader.filter(metadata={'age': {'=': 80}}, include_fields=['disliked'])) == 1
        assert len(reader.filter(metadata={'age': {'<': 80}}, include_fields=['disliked'])) == 3
        assert len(reader.filter(metadata={'age': {'>=': 20}}, include_fields=['disliked'])) == 4
        assert len(
            reader.filter(must=['product'], metadata={'gender': {'=': 'female'}}, include_fields=['disliked'])
        ) == 1

        product_results = reader.filter(must=['product'], include_fields=['disliked'])
        male_gender = reader.filter(metadata={'gender': {'=': 'male'}}, include_fields=['disliked'])
        both_genders = reader.filter(metadata={'gender': {'in': ('female', 'male')}}, include_fields=['disliked'])

        assert len(product_results) == 2
        assert len(composition.exclude(product_results, male_gender)) == 1
        assert len(composition.exclude(product_results, both_genders)) == 0

        # Both fields
        assert len(reader.filter(metadata={'age': {'=': 80}})) == 2
        assert len(reader.filter(metadata={'age': {'<': 80}})) == 6
        assert len(reader.filter(metadata={'age': {'>=': 20}})) == 8
        assert len(reader.filter(must=['product'], metadata={'gender': {'=': 'female'}})) == 2

        product_results = reader.filter(must=['product'])
        male_gender = reader.filter(metadata={'gender': {'=': 'male'}})
        both_genders = reader.filter(metadata={'gender': {'in': ('female', 'male')}})

        assert len(product_results) == 4
        assert len(composition.exclude(product_results, male_gender)) == 2
        assert len(composition.exclude(product_results, both_genders)) == 0

        # Comparing searching against the full metadata:
        all_metadata = {field: values for field, values in reader.get_metadata(None)}
        assert len(all_metadata['gender']['male']) == len(male_gender)


def test_searching_alice(index_dir):
    """Test basic searching functions for Alice."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        f.seek(0)
        data = f.read()
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT(analyser=analyser)))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            assert len(reader.filter(should=['King'])) == 59
            assert len(reader.filter(should=['King', 'Queen'])) == 122 == \
                len(reader.filter(should=['Queen', 'King']))
            assert len(reader.filter(must=['King', 'Queen'])) == 4 == \
                len(composition.match_all(
                    reader.filter(should=['King']), reader.filter(should=['Queen'])
                ))

            assert len(reader.filter(should=['King'], must_not=['Queen'])) == 55

            assert (
                len(reader.filter(must=['Alice', ('thought', 'little')])) == 69 ==
                len(
                    composition.match_all(
                        reader.filter(must=['Alice']),
                        composition.match_any(
                            reader.filter(must=['thought']),
                            reader.filter(must=['little'])))))

            assert len(reader.filter(should=["thistermdoesntexist"])) == 0
            assert len(reader.filter(should=["Mock Turtle"])) == 51

            jury_frames = composition.score_and_rank(reader.filter(must=["jury"]), limit=1)
            assert len(jury_frames) == 1
            frame = reader.get_frame(jury_frames[0][0], None)
            assert "jury" in frame['_text']

            # Look at different variations of scoring and boosting.
            voice_hits = len(reader.filter(should=["voice"]))
            assert voice_hits == 46

            misses = 0
            results = composition.score_and_rank(reader.filter(should=["Alice", "voice"]), limit=voice_hits)
            assert len(results) == voice_hits
            for frame_id, hit in reader.get_frames(None, frame_ids=[i[0] for i in results]):
                misses += (1 if "voice" not in hit['_text'] else 0)
            assert misses == 10

            misses = 0
            results = composition.score_and_rank(
                composition.match_any(
                    reader.filter(should=["Alice"]),
                    composition.boost(reader.filter(should=['voice']), 0.2)
                ),
                limit=voice_hits
            )
            assert len(results) == voice_hits
            for frame_id, hit in reader.get_frames(None, frame_ids=[i[0] for i in results]):
                misses += (1 if "voice" not in hit['_text'] else 0)
            assert misses == 35

            misses = 0
            results = composition.score_and_rank(
                composition.match_any(
                    reader.filter(should=["Alice"]),
                    composition.boost(reader.filter(should=['voice']), 0.6)
                ),
                limit=voice_hits
            )
            assert len(results) == voice_hits
            for frame_id, hit in reader.get_frames(None, frame_ids=[i[0] for i in results]):
                misses += (1 if "voice" not in hit['_text'] else 0)
            assert misses == 10

            misses = 0
            results = composition.score_and_rank(
                composition.match_any(
                    reader.filter(should=["Alice"]),
                    composition.boost(reader.filter(should=['voice']), 20)
                ),
                limit=voice_hits
            )
            assert len(results) == voice_hits
            for frame_id, hit in reader.get_frames(None, frame_ids=[i[0] for i in results]):
                misses += (1 if "voice" not in hit['_text'] else 0)
            assert misses == 0

            # No limits for the search results - the lowest scored correspond to the most frequent
            # term - Alice
            misses = 0
            results = composition.score_and_rank(reader.filter(should=["Alice", "voice"]), limit=0)
            for frame_id, hit in reader.get_frames(None, frame_ids=[i[0] for i in results[-voice_hits:]]):
                misses += (1 if "voice" not in hit['_text'] else 0)
            assert misses == voice_hits

            misses = 0
            results = composition.score_and_rank(
                composition.match_any(
                    reader.filter(should=["voice"]),
                    composition.boost(reader.filter(should=['Alice']), 20)
                ),
                limit=0
            )

            for frame_id, hit in reader.get_frames(None, frame_ids=[i[0] for i in results[-voice_hits:]]):
                misses += (1 if "voice" not in hit['_text'] else 0)
            assert misses == 11

            results = reader.filter(should=["King"], must_not=['court', 'evidence'])
            scored = composition.score_and_rank(results, limit=25)
            assert len(scored) == 25
            assert len(results) == 52 == len(composition.exclude(
                reader.filter(should=["King"]),
                reader.filter(should=['court', 'evidence'])
            ))

            for frame_id, hit in reader.get_frames(None, frame_ids=[i[0] for i in scored]):
                assert "evidence" not in hit['_text']
                assert "court" not in hit['_text']
                assert hit['_field'] == 'text'
                assert all([i in hit for i in ('_id', '_doc_id')])

            with pytest.raises(TypeError):
                # Invalid query format
                reader.filter(['hello', 'text'])


def test_searching_alice_simple(index_dir):
    """Test searching for Alice with the simple scorer."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        f.seek(0)
        data = f.read()
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT(analyser=analyser)))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            results1 = reader.filter(should=['Alice', 'Caterpillar'])
            # Should be the same as results1 - supported for convenience
            results2 = reader.filter(should=[('Alice', 'Caterpillar')])
            # variations on a term should be the same in this context
            results3 = reader.filter(must=[('Alice', 'Caterpillar')])

            assert results1.keys() == results2.keys() == results3.keys()

            for frame_id, frame in reader.get_frames(None, frame_ids=results1):
                assert 'Alice' in frame['_text'] or 'Caterpillar' in frame['_text']

            # Now score, rank and limit
            scored_results = composition.score_and_rank(results1, limit=25)
            assert len(scored_results) == 25
            matching_frames = list(
                reader.get_frames(None, frame_ids=[scored_results[0][0], scored_results[-1][0]])
            )
            assert 'Alice' in matching_frames[0][1]['_text'] and 'Caterpillar' in matching_frames[0][1]['_text']
            assert 'Alice' not in matching_frames[-1][1]['_text']


def test_searching_mt_warning(index_dir):
    """Test searching for mt warning data."""
    with open(os.path.abspath('caterpillar/test_resources/mt_warning_utf8.txt'), 'rbU') as f:
        data = f.read()
        analyser = TestAnalyser()
        config = IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT(analyser=analyser)))
        with IndexWriter(index_dir, config) as writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            assert len(reader.filter(should=['1770'])) == 2
            assert len(reader.filter(should=['1,900'])) == 1
            assert len(reader.filter(should=['4.4'])) == 1


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
            assert len(reader.filter(should=['@NYSenate'])) == 1
            assert len(reader.filter(should=['summerdays@gmail.com'])) == 1

            assert (
                len(reader.filter(metadata={'sentiment': {'=': 'positive'}})) +
                len(reader.filter(metadata={'sentiment': {'=': 'negative'}})) ==
                reader.get_frame_count('text')
            )
