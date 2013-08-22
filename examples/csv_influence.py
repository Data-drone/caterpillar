# caterpillar - extract keterms from a csv file
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import csv
from datetime import datetime
from caterpillar.analytics.influence import InfluenceAnalyticsPlugin
from caterpillar.processing.index import Index
from caterpillar.processing.schema import NUMERIC, Schema, TEXT


with open('examples/big.csv', 'r') as file:
    # Create the index first
    text_index = Index.create(Schema(respondant=NUMERIC, region=TEXT(indexed=False), store=TEXT(indexed=False),
                                     liked=TEXT, disliked=TEXT, would_like=TEXT, nps=NUMERIC))
    csv_reader = csv.reader(file)
    csv_reader.next()  # Skip header
    for row in csv_reader:
        text_index.add_document(update_index=False, respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                disliked=row[4], would_like=row[5], nps=row[6])
    text_index.reindex()
    # Run the influence plugin
    text_index.run_plugin(InfluenceAnalyticsPlugin)
    total_influence = InfluenceAnalyticsPlugin(text_index).get_cumulative_influence_table()
