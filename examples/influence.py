# caterpillar - Calculate term influence from a text file.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.processing.index import Index
from caterpillar.processing.schema import Schema, TEXT


with open('examples/alice.txt', 'r') as f:
    data = f.read()
    text_index = Index.create(Schema(text=TEXT))
    text_index.add_document(fold_case=True, text=data)
    text_index.run_plugin(InfluenceAnalyticsPlugin)
    influence_factors_table = InfluenceAnalyticsPlugin(text_index).get_influence_factors_table()
    term_influence_table = InfluenceAnalyticsPlugin(text_index).get_term_influence_table()
    print influence_factors_table['eat']
    print text_index.get_frequencies()['eat']
