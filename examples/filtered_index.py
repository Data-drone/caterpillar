"""
Create a derived index for a subset of an existing index based on a field query.

"""
import csv
import os

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.processing.index import DerivedIndex, Index
from caterpillar.processing.schema import Schema, CATEGORICAL_TEXT, NUMERIC, TEXT

with open(os.path.abspath('caterpillar/test_resources/nps_medium.csv'), 'rbU') as f:
    index = Index.create(Schema(respondant=NUMERIC, region=CATEGORICAL_TEXT(indexed=True),
                                store=CATEGORICAL_TEXT(indexed=True),
                                liked=TEXT, disliked=TEXT, would_like=TEXT, nps=NUMERIC(indexed=True)))
    csv_reader = csv.reader(f)
    csv_reader.next()  # Skip header
    for row in csv_reader:
        index.add_document(update_index=False, respondant=row[0], region=row[1], store=row[2], liked=row[3],
                           disliked=row[4], would_like=row[5], nps=row[6])

    index.reindex()

filtered_index = DerivedIndex.create_from_composite_query([(index, "nps>=8")])
filtered_index.run_plugin(InfluenceAnalyticsPlugin)

index.destroy()
filtered_index.destroy()
