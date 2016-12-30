# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""Tests for caterpillar.processing.index."""
from __future__ import division

import csv
import pickle
import shutil
import tempfile
import mock
import multiprocessing as mp
import multiprocessing.dummy as mt  # threading dummy with same interface as multiprocessing

import pytest

from caterpillar.storage.sqlite import SqliteStorage
from caterpillar.storage import Storage
from caterpillar.processing.analysis.analyse import EverythingAnalyser
from caterpillar.processing.index import *
from caterpillar.processing.schema import ID, NUMERIC, TEXT, FieldType, Schema
from caterpillar.searching.query.querystring import QueryStringQuery
from caterpillar.test_util import TestAnalyser, TestBiGramAnalyser


def test_index_open(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage,
                             Schema(text1=TEXT(analyser=analyser),
                                    text2=TEXT(analyser=analyser),
                                    document=TEXT(analyser=analyser, indexed=False),
                                    flag=FieldType(analyser=EverythingAnalyser(),
                                    indexed=True, categorical=True))))
        with writer:
            writer.add_document(text1=data, text2=data, document='alice.txt', flag=True, frame_size=2)

        # Identical text fields should generate the same frames and frequencies
        with IndexReader(index_dir) as reader:
            assert sum(1 for _ in reader.get_frequencies('text1')) == 500
            assert sum(1 for _ in reader.get_frequencies('text2')) == 500
            assert reader.get_term_frequency('Alice', 'text1') == 23
            assert reader.get_term_frequency('Alice', 'text2') == 23
            assert reader.get_document_count() == 1
            assert reader.get_frame_count('text1') == 52
            assert reader.get_frame_count('text2') == 52
            assert isinstance(reader.get_schema()['text1'], TEXT)
            assert isinstance(reader.get_schema()['text2'], TEXT)

        # Adding the same document twice should double the frame, term_frequencies and document counts
        with writer:
            writer.add_document(text1=data, text2=data, document='alice.txt', flag=True, frame_size=2)

        with IndexReader(index_dir) as reader:
            assert sum(1 for _ in reader.get_frequencies('text1')) == 500
            assert reader.get_term_frequency('Alice', 'text1') == 46
            assert reader.get_document_count() == 2
            assert reader.get_frame_count('text1') == 104
            assert isinstance(reader.get_schema()['text1'], TEXT)

        path = tempfile.mkdtemp()
        new_dir = os.path.join(path, "no_reader")
        try:
            with pytest.raises(IndexNotFoundError):
                IndexWriter(new_dir, IndexConfig(SqliteStorage, Schema(text=TEXT)))
                IndexReader(new_dir)  # begin() was never called on the writer
            with pytest.raises(IndexNotFoundError):
                with IndexWriter(new_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
                    pass
                os.remove(os.path.join(new_dir, "storage.db"))
                IndexReader(new_dir)  # The written container no longer exists
        finally:
            shutil.rmtree(path)

    with pytest.raises(IndexNotFoundError):
        IndexReader("fake")


def test_index_writer_not_found(index_dir):
    with pytest.raises(IndexNotFoundError):
        IndexWriter(index_dir)


def test_index_settings(index_dir):
    writer = IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT())))
    with writer:
        writer.set_setting('test', True)
        writer.set_setting('is_testing_fun', False)

    with IndexReader(index_dir) as reader:
        assert reader.get_setting('test')
        assert not reader.get_setting('is_testing_fun')
        settings = {k: v for k, v in reader.get_settings(['test', 'is_testing_fun'])}
        assert len(settings) == 2
        assert 'test' in settings
        with pytest.raises(SettingNotFoundError):
            reader.get_setting('dummy')


def test_index_config():
    """Test the IndexConfig object."""
    mock_storage = Storage('nothing', 'doing')
    conf = IndexConfig(mock_storage, Schema())
    assert conf.version == VERSION

    pickle_data = pickle.dumps(True)
    with pytest.raises(ValueError):
        IndexConfig.loads(pickle_data)
    with pytest.raises(ValueError):
        IndexConfig.loads(" ")


def test_index_alice(index_dir):
    """Whole bunch of functional tests on the index."""
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage,
                                                    Schema(text=TEXT(analyser=analyser),
                                                           document=TEXT(analyser=analyser, indexed=False),
                                                           blank=NUMERIC(indexed=True), ref=ID(indexed=True))))
        with writer:
            writer.add_document(text=data, document='alice.txt', blank=None, ref=123, frame_size=2)

        with IndexReader(index_dir) as reader:
            assert sum(1 for _ in reader.get_term_positions('nice', 'text')) == 3
            assert sum(1 for _ in reader.get_term_positions('key', 'text')) == 5

            assert reader.get_term_association('Alice', 'poor', 'text') == \
                reader.get_term_association('poor', 'Alice', 'text') == 3
            assert reader.get_term_association('key', 'golden', 'text') == \
                reader.get_term_association('golden', 'key', 'text') == 3

            assert reader.get_vocab_size('text') == sum(1 for _ in reader.get_frequencies('text')) == 500
            assert reader.get_term_frequency('Alice', 'text') == 23

            # Make sure this works
            reader.__sizeof__()

        with IndexWriter(index_dir) as writer:
            writer.add_fields(field1=TEXT, field2=NUMERIC)

        with IndexReader(index_dir) as reader:
            schema = reader.get_schema()
            assert 'field1' in schema
            assert 'field2' in schema

        with IndexWriter(index_dir) as writer:
            writer.delete_document(1)

        with IndexReader(index_dir) as reader:
            with pytest.raises(DocumentNotFoundError):
                reader.get_document(1)

        # with IndexWriter(index_dir) as writer:
        #     with pytest.raises(DocumentNotFoundError):
        #         writer.delete_document(1)

        with IndexReader(index_dir) as reader:
            assert 'Alice' not in reader.get_frequencies('text')
            assert 'Alice' not in reader.get_associations_index('text')
            assert 'Alice' not in reader.get_positions_index('text')

        # Test not text
        with IndexWriter(index_dir) as writer:
            with pytest.raises(TypeError):
                writer.add_document(text=False, document='alice', frame_size=0)

        # Test frame size = 0 (whole document)
        with IndexWriter(index_dir) as writer:
            writer.add_document(text=data, document='alice', frame_size=0)
            writer.add_document(text=unicode("unicode data"), document='test', frame_size=0)

        with IndexReader(index_dir) as reader:
            assert reader.get_frame_count('text') == 2


def test_index_writer_rollback(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser))))
        writer.begin()
        try:
            writer.add_document(text=data)
        finally:
            writer.close()

        with IndexReader(index_dir) as reader:
            assert reader.get_document_count() == 0

        # Test rollback on exception
        try:
            with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser)))) as writer:
                writer.add_document(text=data)
                raise ValueError()
        except ValueError:
            pass

        with IndexReader(index_dir) as reader:
            assert reader.get_document_count() == 0


def test_index_writer_lock(index_dir):
    analyser = TestAnalyser()
    with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser)))) as writer1:
        writer1.add_document(text="Blah")
        writer2 = IndexWriter(index_dir)
        with pytest.raises(IndexWriteLockedError):
            writer2.begin(timeout=0.5)


def test_index_frames_docs_alice(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage,
                                                    Schema(text=TEXT(analyser=analyser),
                                                           document=TEXT(analyser=analyser, indexed=False))))
        with writer:
            writer.add_document(text=data, document='alice.txt', frame_size=2)

        with IndexReader(index_dir) as reader:
            assert reader.get_frame_count('text') == 52

            frame_id = reader.get_term_positions('Alice', 'text').keys()[0]
            frame = reader.get_frame(frame_id, 'text')
            assert frame['_id'] == frame_id

            doc_id = frame['_doc_id']
            assert doc_id == reader.get_document(doc_id)['_id']
            assert doc_id == next(reader.get_documents())[0]


def test_index_moby_small(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/moby_small.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser))))
        with writer:
            writer.add_document(text=data, frame_size=2, )

        with IndexReader(index_dir) as reader:
            assert sum(1 for _ in reader.get_term_positions('Mr. Chace', 'text')) == 1
            assert sum(1 for _ in reader.get_term_positions('CONVERSATIONS', 'text')) == 1
            assert sum(1 for _ in reader.get_frequencies('text')) == 38


def test_index_alice_bigram_discovery(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            bi_grams = find_bi_gram_words(reader.get_frames('text'))
            assert len(bi_grams) == 4
            assert 'golden key' in bi_grams


def test_moby_bigram_discovery(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/moby.txt'), 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            bi_grams = find_bi_gram_words(reader.get_frames('text'))
            assert len(bi_grams) == 10
            assert 'steering oar' in bi_grams


def test_wikileaks_bigram_discovery(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/wikileaks-secret.txt'), 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            bi_grams = find_bi_gram_words(reader.get_frames('text'))
            assert len(bi_grams) == 29


def test_employee_survet_bigram_discovery(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/government-emplyee-survey-PC.txt'), 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            bi_grams = find_bi_gram_words(reader.get_frames('text'))
            assert len(bi_grams) == 7


def test_index_alice_merge_bigram(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'r') as f:
        f.seek(0)
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(text=data)
        with IndexReader(index_dir) as reader:
            bi_grams = find_bi_gram_words(reader.get_frames('text'), min_count=3)

        bigram_index = os.path.join(tempfile.mkdtemp(), "bigram")
        merge_index = os.path.join(tempfile.mkdtemp(), "merge")
        try:
            analyser = TestBiGramAnalyser(bi_grams.keys(), )
            with IndexWriter(bigram_index, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser)))) as writer:
                writer.add_document(text=data)
                # Quick plumbing test.
                with pytest.raises(ValueError):
                    writer._merge_terms_into_ngram("old", None, {}, {}, {}, {})

            terms_to_merge = [[b.split(' '), b] for b in bi_grams]

            # Potential bi-grams 'white kid' & 'kid gloves' form the tri-gram 'white kid gloves'.
            # The bi-gram analyser will match 'white kid' first due to lexical order, consuming the 'kid' token.
            # Subsequently, 'kid gloves' will not be matched. This manual test using `terms_to_merge` however, attempts
            # to convert the bi-grams in alphabetic order, hence 'kid gloves' will be matched first.
            for i, m in enumerate(terms_to_merge):
                if m[1] == 'kid gloves':
                    break
            # ...so, here we just re-order 'kid gloves' to the end of `merges`
            # to emulate the behavior of the bi-gram analyser
            terms_to_merge.append(terms_to_merge.pop(i))

            analyser = TestAnalyser()
            with IndexWriter(merge_index, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser)))) as writer:
                writer.add_document(text=data)
            with IndexWriter(merge_index) as writer:
                writer.materialise_phrases(bi_grams)

            # Verify indexes match
            with IndexReader(merge_index) as merges, IndexReader(bigram_index) as bigrams:
                # Frequencies
                assert bigrams.get_term_frequency('golden key', 'text') == 6
                assert bigrams.get_term_frequency('golden', 'text') == 1
                assert bigrams.get_term_frequency('key', 'text') == 3
                merge_frequencies = {k: v for k, v in merges.get_frequencies('text')}
                merge_associations = {k: v for k, v in merges.get_associations_index('text')}
                for term, frequency in bigrams.get_frequencies('text'):
                    assert merge_frequencies[term] == frequency
                # Associations
                merge_associations = {k: v for k, v in merges.get_associations_index('text')}
                for term, associations in bigrams.get_associations_index('text'):
                    assert merge_associations[term] == associations
                # Frame positions
                frame_mappings = {}
                merge_frames = sorted({k: v for k, v in merges.get_frames('text')}.values(),
                                      key=lambda t: t['_sequence_number'])
                bigram_frames = sorted({k: v for k, v in bigrams.get_frames('text')}.values(),
                                       key=lambda t: t['_sequence_number'])
                for i, merge_frame in enumerate(merge_frames):
                    frame_mappings[bigram_frames[i]['_id']] = merge_frame['_id']
                    assert merge_frame['_positions'] == bigram_frames[i]['_positions']
                # Global positions
                merge_positions = {k: v for k, v in merges.get_positions_index('text')}
                for term, positions in bigrams.get_positions_index('text'):
                    for f_id, f_positions in positions.iteritems():
                        assert f_positions == merge_positions[term][frame_mappings[f_id]]

                with pytest.raises(Exception):
                    merges.merge_terms([[('hot', 'dog',), '']], 'text')

            with IndexWriter(merge_index) as writer:
                writer.merge_terms([[('garbage', 'term',), 'test']], 'text')
                writer.merge_terms([[('Alice', 'garbage',), 'test']], 'text')
            with IndexReader(merge_index) as reader:
                with pytest.raises(KeyError):
                    reader.get_term_frequency('garbage term', 'text')
                with pytest.raises(KeyError):
                    reader.get_term_frequency('Alice garbage', 'text')
        finally:
            shutil.rmtree(bigram_index)
            shutil.rmtree(merge_index)


def test_index_moby_case_folding(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/moby.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser))))
        with writer:
            writer.add_document(text=data, frame_size=2)

        with writer:
            writer.fold_term_case('text')

        with IndexReader(index_dir) as reader:
            with pytest.raises(KeyError):
                reader.get_term_positions('flask', 'text')
            with pytest.raises(KeyError):
                assert not reader.get_term_frequency('flask', 'text')
            assert reader.get_term_frequency('Flask', 'text') == 88
            assert reader.get_term_association('Flask', 'person', 'text') == \
                reader.get_term_association('person', 'Flask', 'text') == 2

            with pytest.raises(KeyError):
                reader.get_term_positions('Well', 'text')
            with pytest.raises(KeyError):
                assert not reader.get_term_frequency('Well', 'text')
            assert reader.get_term_frequency('well', 'text') == 194
            assert reader.get_term_association('well', 'whale', 'text') == \
                reader.get_term_association('whale', 'well', 'text') == 20

            with pytest.raises(KeyError):
                reader.get_term_positions('Whale', 'text')
            with pytest.raises(KeyError):
                assert not reader.get_term_frequency('Whale', 'text')
            assert reader.get_term_frequency('whale', 'text') == 695
            assert reader.get_term_association('whale', 'American', 'text') == \
                reader.get_term_association('American', 'whale', 'text') == 9

            assert reader.get_term_frequency('T. HERBERT', 'text') == 1
            assert sum(1 for _ in reader.get_frequencies('text')) == 20542


def test_index_merge_terms(index_dir):
    """Test merging terms together."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser))))
        with writer:
            writer.add_document(text=data, frame_size=2)

        with IndexReader(index_dir) as reader:
            assert reader.get_term_frequency('alice', 'text') == 86
            assert reader.get_term_association('alice', 'creatures', 'text') == 1
            assert sum(1 for _ in reader.get_term_positions('alice', 'text')) == 86

            assert reader.get_term_frequency('party', 'text') == 8
            assert reader.get_term_association('party', 'creatures', 'text') == 1
            assert reader.get_term_association('party', 'assembled', 'text') == 1
            assert sum(1 for _ in reader.get_term_positions('party', 'text')) == 8

        writer = IndexWriter(index_dir)
        with writer:
            writer.merge_terms(merges=[
                ('Alice', '',),  # delete
                ('alice', 'tplink',),  # rename
                ('Eaglet', 'party',),  # merge
                ('idonotexist', '',),  # non-existent term
            ], text_field='text')

        with IndexReader(index_dir) as reader:
            with pytest.raises(KeyError):
                reader.get_term_frequency('Alice', 'text')
            with pytest.raises(KeyError):
                reader.get_term_positions('Alice', 'text')

            assert reader.get_term_frequency('tplink', 'text') == 86
            assert reader.get_term_association('tplink', 'creatures', 'text') == 1
            assert sum(1 for _ in reader.get_term_positions('tplink', 'text')) == 86

            assert reader.get_term_frequency('party', 'text') == 10
            assert reader.get_term_association('party', 'creatures', 'text') == 1
            assert reader.get_term_association('party', 'assembled', 'text') == 1
            assert sum(1 for _ in reader.get_term_positions('party', 'text')) == 10


def test_index_alice_case_folding(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage,
                                                    Schema(text=TEXT(analyser=analyser),
                                                           document=TEXT(analyser=analyser, indexed=False))))
        with writer:
            writer.add_document(text=data, document='alice.txt', frame_size=2)
            writer.fold_term_case('text')

        with IndexReader(index_dir) as reader:
            positions_index = {k: v for k, v in reader.get_positions_index('text')}
            for frame_id, frame in reader.get_frames('text'):
                for term in frame['_positions']:
                    assert frame_id in positions_index[term]

            # Check that associations never exceed frequency of either term
            associations = {k: v for k, v in reader.get_associations_index('text')}
            frequencies = {k: v for k, v in reader.get_frequencies('text')}
            for term, term_associations in associations.iteritems():
                for other_term, assoc in term_associations.items():
                    assert assoc <= frequencies[term] and assoc <= frequencies[other_term]

            # Check frequencies against positions
            frequencies = {k: v for k, v in reader.get_frequencies('text')}
            for term, freq in frequencies.items():
                assert freq == len(positions_index[term])


def test_index_case_fold_no_new_term(index_dir):
    """
    Test a dataset that has only uppercase occurrences of a term where it mostly appears at the start of the 1 word
    sentence. This results in those occurrences being converted to lower case (because they are at the start of a
    sentence) then we attempt to merge with the 1 upper case occurrence. Previously we wrongly assumed in the merge code
    that all terms would have an existing entry in the associations matrix but this isn't this case with this tricky
    dataset.

    """
    with open(os.path.abspath('caterpillar/test_resources/case_fold_no_assoc.csv'), 'rbU') as f:
        analyser = TestAnalyser()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser)))) as writer:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                writer.add_document(text=row[0])
            writer.fold_term_case('text')

        with IndexReader(index_dir) as reader:
            assert reader.get_term_frequency('stirling', 'text') == 6
            with pytest.raises(KeyError):
                reader.get_term_frequency('Stirling', 'text')


def test_index_utf8(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/mt_warning_utf8.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage,
                                                    Schema(text=TEXT(analyser=analyser),
                                                           document=TEXT(analyser=analyser, indexed=False))))
        with writer:
            doc_id = writer.add_document(text=data, document='mt_warning_utf8.txt', frame_size=2)
            assert doc_id


def test_index_latin1(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/mt_warning_latin1.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage,
                                                    Schema(text=TEXT(analyser=analyser),
                                                           document=TEXT(analyser=analyser, indexed=False))))
        with writer:
            doc_id = writer.add_document(text=data, document='mt_warning_latin1.txt', frame_size=2, encoding='latin1')
            assert doc_id

            with pytest.raises(IndexError):
                # Bad encoding
                writer.add_document(text=data, document='mt_warning_latin1.txt', frame_size=2)

            # Ignore bad encoding errors
            writer.add_document(text=data, document='mt_warning_latin1.txt', frame_size=2, encoding_errors='ignore')


def test_index_encoding(index_dir):
    analyser = TestAnalyser()
    writer = IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT(analyser=analyser))))
    with writer:
        doc_id = writer.add_document(text=u'This is a unicode string to test our field decoding.', frame_size=2)
        assert doc_id is not None

        with open(os.path.abspath('caterpillar/test_resources/mt_warning_utf8.txt'), 'r') as f:
            data = f.read()
        with pytest.raises(IndexError):
            writer.add_document(text=data, frame_size=2, encoding='ascii')


def test_index_state(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/detractors.csv'), 'rbU') as f:
        csv_reader = csv.reader(f)
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            row = csv_reader.next()
            writer.add_document(text=row[0])

        with IndexReader(index_dir) as reader:
            start_revision = reader.get_revision()

        with writer:
            doc_ids = []
            for row in csv_reader:
                doc_ids.append(writer.add_document(text=row[0]))

        with IndexReader(index_dir) as reader:
            assert start_revision != reader.get_revision()
            revision = reader.get_revision()

        writer = IndexWriter(index_dir, Schema(text=TEXT))
        with writer:
            writer.delete_document(doc_ids[0])

        with IndexReader(index_dir) as reader:
            assert revision != reader.get_revision()


def test_index_reader_writer_isolation(index_dir):
    """Test that readers and writers are isolated."""
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT)))
        with writer:
            writer.add_document(text=data)

        reader = IndexReader(index_dir)
        reader.begin()

        assert reader.get_frame_count('text') == 52
        assert reader.get_term_frequency('Alice', 'text') == 23

        # Add another copy of Alice
        writer = IndexWriter(index_dir, Schema(text=TEXT))
        with writer:
            writer.add_document(text=data)

        # Check reader can't see it
        assert reader.get_frame_count('text') == 52
        assert reader.get_term_frequency('Alice', 'text') == 23

        # Open new reader and make sure it CAN see the changes
        with IndexReader(index_dir) as reader1:
            assert reader1.get_frame_count('text') == reader.get_frame_count('text') * 2
            assert reader1.get_term_frequency('Alice', 'text') == reader.get_term_frequency('Alice', 'text') * 2

        reader.close()


def test_index_document_delete(index_dir):
    """Sanity test for delete document."""
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(text=data)
            writer.add_document(text=data)

        with IndexReader(index_dir) as reader:
            assert reader.get_frame_count('text') == 104
            assert reader.get_term_frequency('Alice', 'text') == 46

        with IndexWriter(index_dir) as writer:
            writer.delete_document(1)

        with IndexReader(index_dir) as reader:
            assert reader.get_frame_count('text') == 52
            assert reader.get_term_frequency('Alice', 'text') == 23


def test_index_multi_document_delete(index_dir):
    """Sanity test for deleting multiple documents."""
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        doc_ids = []
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            doc_ids.append(writer.add_document(text=data))
            doc_ids.append(writer.add_document(text=data))

        with IndexReader(index_dir) as reader:
            assert reader.get_frame_count('text') == 104
            assert reader.get_document_count() == 2

        with IndexWriter(index_dir) as writer:
            for doc_id in doc_ids:
                writer.delete_document(doc_id)

        with IndexReader(index_dir) as reader:
            assert reader.get_frame_count('text') == 0
            assert reader.get_document_count() == 0


def test_metadata_only_retrieval_deletion(index_dir):
    """Test we can retrieve metadata only documents"""
    config = IndexConfig(SqliteStorage, schema=Schema(num=NUMERIC(indexed=True), text=TEXT))
    with IndexWriter(index_dir, config) as writer:
        doc_id = writer.add_document(num=1)

    with IndexReader(index_dir) as reader:
        searcher = reader.searcher()
        assert searcher.count(QueryStringQuery('num=1')) == 1

    with IndexWriter(index_dir, config) as writer:
        writer.delete_document(doc_id)

    with IndexReader(index_dir) as reader:
        assert reader.get_frame_count('') == 0
        assert reader.get_document_count() == 0


def test_field_names(index_dir):
    """Test we can retrieve metadata only documents"""
    schema = {"Don't Like": TEXT, "Would Like": TEXT}
    document1 = {"Don't Like": 'The scenery was unpleasant.', "Would Like": 'More cats.'}
    config = IndexConfig(SqliteStorage, schema=Schema(**schema))

    with IndexWriter(index_dir, config) as writer:
        writer.add_document(document1)

    with IndexReader(index_dir) as reader:
        assert reader.get_document_count() == 1


def _acquire_write_single_doc(index_dir):
    """Write a single document."""
    with IndexWriter(index_dir) as writer:
        writer.add_document(field1='test some text')


def _read_document_count(index_dir):
    """Read something from the reader"""
    with IndexReader(index_dir) as reader:
        reader.get_document_count()


def test_concurrent_write_contention(index_dir):
    """Test that high contention for write lock can still proceed. """

    config = IndexConfig(SqliteStorage, schema=Schema(field1=TEXT))

    # initialise
    with IndexWriter(index_dir, config):
        pass

    # Easier case: contention from threads in the same process
    n = 100
    pool = mt.Pool(16)
    pool.map(_acquire_write_single_doc, [index_dir] * n)
    pool.close()

    # Attempt high contention document writes
    pool = mp.Pool(16)
    pool.map(_acquire_write_single_doc, [index_dir] * n)
    pool.close()

    with IndexReader(index_dir) as reader:
        assert reader.get_document_count() == 200


def test_concurrent_read_write_contention(index_dir):
    """Test that high contention for write lock can still proceed. """

    config = IndexConfig(SqliteStorage, schema=Schema(field1=TEXT))

    # initialise
    with IndexWriter(index_dir, config):
        pass

    # Start writing a document, and at the same time, attempt to read
    # multiple times in parallel. Eventually one of these will overlap
    # the writer closing.
    pool = mp.Pool(16)

    for i in range(100):
        pool.apply_async(_acquire_write_single_doc, (index_dir,))
        pool.map(_read_document_count, [index_dir] * 100)
    pool.close()
