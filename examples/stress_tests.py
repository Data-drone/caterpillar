# caterpillar - Some index stress testing examples.
#
# Copyright (C) 2012-2014 Mammoth Labs
# Author: Kris Rogers <kris@kapiche.com>
import csv

from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.processing.index import Index
from caterpillar.processing.schema import TEXT, Schema, NUMERIC

# Big frames
with open('examples/alice.txt', 'r') as file:
    data = file.read()
    text_index = Index.create(Schema(text=TEXT))
    text_index.add_document(frame_size=0, fold_case=True, text=data)

# Lots of frames, big vocabulary
text_index = Index.create(Schema(text=TEXT))
with open('caterpillar/test_resources/twitter_sentiment.csv', 'r') as file:
    csv_reader = csv.reader(file)
    csv_reader.next()  # Skip header
    for row in csv_reader:
        text_index.add_document(update_index=False, text=row[1])
with open('caterpillar/test_resources/promoters.csv', 'r') as file:
    csv_reader = csv.reader(file)
    csv_reader.next()  # Skip header
    for row in csv_reader:
        text_index.add_document(update_index=False, text=row[0])
with open('caterpillar/test_resources/detractors.csv', 'r') as file:
    csv_reader = csv.reader(file)
    csv_reader.next()  # Skip header
    for row in csv_reader:
        text_index.add_document(update_index=False, text=row[0])
with open('caterpillar/test_resources/moby.txt', 'r') as file:
    data = file.read()
    text_index.add_document(fold_case=True, text=data)
with open('caterpillar/test_resources/alice.txt', 'r') as file:
    data = file.read()
    text_index.add_document(fold_case=True, text=data)
text_index.reindex()
# Run the influence plugin
text_index.run_plugin(InfluenceAnalyticsPlugin)
influence_factors = InfluenceAnalyticsPlugin(text_index).get_influence_factors_table()