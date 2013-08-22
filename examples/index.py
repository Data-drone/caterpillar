# caterpillar - Create an index from a file
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.processing.index import Index
from caterpillar.processing.schema import TEXT, Schema, NUMERIC

with open('examples/moby.txt', 'r') as file:
    data = file.read()
    text_index = Index.create(Schema(text=TEXT, some_number=NUMERIC))
    text_index.add_document(fold_case=True, text=data, some_number=1)
