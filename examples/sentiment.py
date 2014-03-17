# caterpillar - Calculate term influence from a text file.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.analytics.influence import InfluenceAnalyticsPlugin, InfluenceTopicsPlugin
from caterpillar.analytics.sentiment import SentimentPlugin
from caterpillar.processing.index import Index
from caterpillar.processing.schema import Schema, TEXT


with open('examples/alice.txt', 'r') as f:
    data = f.read()
    text_index = Index.create(Schema(text=TEXT))
    text_index.add_document(fold_case=True, text=data)
    influence_plugin = text_index.run_plugin(InfluenceAnalyticsPlugin)
    influence_factors_table = influence_plugin.get_influence_factors_table()
    term_influence_table = influence_plugin.get_term_influence_table()
    topics_plugin = text_index.run_plugin(InfluenceTopicsPlugin)
    sentiment_plugin = text_index.run_plugin(SentimentPlugin, topics=topics_plugin.get_topic_frames())
