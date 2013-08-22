# caterpillar - Calculate topical classification from a text file.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import os
from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import Index
from caterpillar.processing.schema import Schema, TEXT


with open(os.path.abspath('caterpillar/resources/alice.txt'), 'r') as f:
    index = Index.create(Schema(text=TEXT(analyser=DefaultAnalyser())))
    data = f.read()
    index.add_document(fold_case=True, text=data)
    index.run_plugin(InfluenceAnalyticsPlugin,
                     influence_contribution_threshold=3.841,
                     cumulative_influence_smoothing=True)
    topical_classification = InfluenceAnalyticsPlugin(index).get_topical_classification()

