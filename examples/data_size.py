# caterpillar - Compare the performance of RAM with SQL storage
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import csv
import os
import time

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.data.sqlite import SqliteStorage, SqliteMemoryStorage
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import Index
from caterpillar.processing.schema import Schema, TEXT, NUMERIC, CATEGORICAL_TEXT, ID


with open('examples/big.csv', 'r') as f:
    text_index = Index.create(Schema(respondant=ID, region=CATEGORICAL_TEXT, liked=TEXT, nps=NUMERIC),
                              storage_cls=SqliteStorage, path='/tmp')
    csv_reader = csv.reader(f)
    csv_reader.next()  # Skip header
    likes_size = 0
    rows = 0
    for row in csv_reader:
        text_index.add_document(update_index=False, respondant=row[0], region=row[1], liked=row[3], nps=row[6])
        rows += 1
        likes_size += len(row[3])
    text_index.reindex()
    # Run the influence plugin
    text_index.run_plugin(InfluenceAnalyticsPlugin)
    total_influence = InfluenceAnalyticsPlugin(text_index).get_topical_classification(influence_threshold=3.841)
    print likes_size/rows
