# caterpillar - Create an index from a file
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.processing.analysis.analyse import BiGramAnalyser
from caterpillar.processing.index import Index, find_bi_gram_words
from caterpillar.processing.schema import Schema, TEXT
from caterpillar.processing.frames import frame_stream

with open('examples/alice.txt', 'r') as f:
    bi_grams = find_bi_gram_words(frame_stream(f))
    f.seek(0)
    data = f.read()
    index = Index.create(Schema(text=TEXT(analyser=BiGramAnalyser(bi_grams))))
    index.add_document(text=data, frame_size=2, fold_case=True, update_index=True)