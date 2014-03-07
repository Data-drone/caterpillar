"""
Create a derived index that is composed from the results of queries against two separate pre-existing indices.

"""
import csv
import os

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.processing.index import DerivedIndex, Index
from caterpillar.processing.schema import Schema, TEXT


with open(os.path.abspath('caterpillar/test_resources/detractors.csv'), 'rbU') as f:
    index1 = Index.create(Schema(text=TEXT))
    csv_reader = csv.reader(f)
    for row in csv_reader:
        index1.add_document(update_index=False, text=row[0])
    index1.reindex()

with open(os.path.abspath('caterpillar/test_resources/promoters.csv'), 'rbU') as f:
    index2 = Index.create(Schema(text=TEXT))
    csv_reader = csv.reader(f)
    for row in csv_reader:
        index2.add_document(update_index=False, text=row[0])
    index2.reindex()

index = DerivedIndex.create_from_composite_query([(index1, "service"), (index2, "service")])
index.run_plugin(InfluenceAnalyticsPlugin)

index.destroy()
index1.destroy()
index2.destroy()
