# Copyright (C) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import os
import sys
from caterpillar.analytics.influence import InfluenceAnalyticsPlugin, InfluenceTopicsPlugin, CodebookTopicsPlugin
from caterpillar.data.sqlite import SqliteMemoryStorage
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import Index
from caterpillar.processing.schema import Schema, TEXT


storage_cls = SqliteMemoryStorage
with open(os.path.abspath('../caterpillar/test_resources/alice.txt'), 'r') as f:
    analyser = DefaultAnalyser()
    index = Index.create(Schema(text=TEXT(analyser=analyser)), storage_cls=storage_cls)
    data = f.read()
    index.add_document(text=data)
    index.fold_term_case()

    # Generate automatic codebook
    index.run_plugin(InfluenceAnalyticsPlugin, influence_factor_smoothing=True)
    topical_classification = index.run_plugin(InfluenceTopicsPlugin).get_topical_classification()
    num_topics = len(topical_classification.topics)
    orig_codebook = CodebookTopicsPlugin.generate_codebook(topical_classification)

    # Remove everything bar topic names
    modified_codebook = dict()
    for topic, terms in orig_codebook.iteritems():
        modified_codebook[topic] = dict()
        modified_codebook[topic]['secondary_terms'] = []
        modified_codebook[topic]['primary_terms'] = terms['primary_terms']

    # Regen from the modified codebook
    topics_plugin = index.run_plugin(CodebookTopicsPlugin, codebook=modified_codebook, add_detail_terms=True)
    new_codebook = CodebookTopicsPlugin.generate_codebook(topics_plugin.get_topical_classification())

    # Compare them
    for topic, terms in orig_codebook.iteritems():
        sys.stdout.write('Testing topic {}.....'.format(topic))
        assert len(set(new_codebook[topic]['primary_terms']).difference(terms['primary_terms'])) == 0
        assert len(set(new_codebook[topic]['secondary_terms']).difference(terms['secondary_terms'])) == 0
        print 'Topic {} matches!'.format(topic)
    assert len(orig_codebook) == len(new_codebook)
    assert len(set(orig_codebook.keys()).difference(new_codebook.keys())) == 0
    print 'Codebooks match!'
