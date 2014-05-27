# caterpillar - Calculate topical classification from a text file.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import os
from caterpillar.analytics.influence import InfluenceAnalyticsPlugin, InfluenceTopicsPlugin
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import Index
from caterpillar.processing.schema import Schema, TEXT


with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'r') as f:
    index = Index.create(Schema(text=TEXT(analyser=DefaultAnalyser())))
    data = f.read()
    index.add_document(fold_case=True, text=data)
    index.run_plugin(InfluenceAnalyticsPlugin, influence_factor_smoothing=True)
    topics_plugin = index.run_plugin(InfluenceTopicsPlugin)
    topical_classification = topics_plugin.get_topical_classification()

    for topic in topical_classification.topics:
        print topic.name
        for term in topic.terms:
            print '\t{}'.format(term.value), round(topic.term_statistics[term.value].relative_frequency, 3),\
                round(topic.term_statistics[term.value].relative_influence, 3)
        print '\n'
