# caterpillar - Search an index
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
from caterpillar.processing.index import Index
from caterpillar.processing.schema import TEXT, Schema

with open('examples/alice.txt', 'r') as f:
    data = f.read()
    text_index = Index.create(Schema(text=TEXT))
    text_index.add_document(fold_case=True, text=data)

    searcher = text_index.searcher()

    results = searcher.search('W*e R?bbit and (thought or little^1.5)')
    print "Retrieved {} of {} matches for {}".format(len(results), results.num_matches, results.query)
