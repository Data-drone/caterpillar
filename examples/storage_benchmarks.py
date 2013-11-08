# caterpillar - Compare the performance of RAM with SQL storage
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import os
import time

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.data.sqlite import SqliteStorage, SqliteMemoryStorage
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.index import Index
from caterpillar.processing.schema import Schema, TEXT


with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
    data = f.read()

# Memory Storage
ram_start = time.time()
index = Index.create(Schema(text=TEXT(analyser=DefaultAnalyser()),
                            document=TEXT(analyser=DefaultAnalyser(), indexed=False)),
                     storage_cls=SqliteMemoryStorage)
index.add_document(text=data, document='moby.txt', frame_size=2, fold_case=True, update_index=True)
index.run_plugin(InfluenceAnalyticsPlugin, influence_contribution_threshold=3.841, cumulative_influence_smoothing=False)
topical_classification = InfluenceAnalyticsPlugin(index).get_topical_classification(influence_threshold=3.841)
ram_latency = time.time() - ram_start

# Sqlite
sql_start = time.time()
index = Index.create(Schema(text=TEXT(analyser=DefaultAnalyser()),
                            document=TEXT(analyser=DefaultAnalyser(), indexed=False)),
                     storage_cls=SqliteStorage, path=os.getcwd())
index.add_document(text=data, document='moby.txt', frame_size=2, fold_case=True, update_index=True)
index.run_plugin(InfluenceAnalyticsPlugin, influence_contribution_threshold=3.841, cumulative_influence_smoothing=False)
topical_classification = InfluenceAnalyticsPlugin(index).get_topical_classification(influence_threshold=3.841)
sql_latency = time.time() - sql_start
index.destroy()

print 'Ram time: {}s\nSQL time: {}s'.format(ram_latency, sql_latency)

"""
import cProfile, StringIO, pstats
pr = cProfile.Profile()
pr.enable()
with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
    data = f.read()
index = Index.create(Schema(text=TEXT(analyser=DefaultAnalyser())),
                     storage_cls=SqliteStorage, path=os.getcwd())
index.add_document(text=data, frame_size=2, fold_case=True, update_index=True)
index.run_plugin(InfluenceAnalyticsPlugin, influence_contribution_threshold=3.841, cumulative_influence_smoothing=False)
topical_classification = InfluenceAnalyticsPlugin(index).get_topical_classification(influence_threshold=3.841)
index.destroy()
pr.disable()
s = StringIO.StringIO()
sortby = 'cumulative'
ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
ps.print_stats()
print s.getvalue()
"""
