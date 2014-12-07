# Copyright (c) 2012-2014 Kapiche Limited
# Aurthor: Ryan Stuart <ryan@kapiche.com>
"""
Read in a bunch of pre-parsed csv files produced from the Wikipedia XML dump (1 article per row) and turn them into an
Lucene index.
"""
import cProfile
from collections import namedtuple
import csv
from datetime import datetime
import os
import pstats
import sys

import begin
import lucene
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version


INDEX_DIR = "IndexFiles.index"


def index(index_dir, f):
    start = datetime.now()
    if not os.path.exists(index_dir):
        os.makedirs(index_dir)

    store = SimpleFSDirectory(File(index_dir))
    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
    config = IndexWriterConfig(Version.LUCENE_CURRENT, analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    title = FieldType()
    title.setIndexed(True)
    title.setStored(True)
    title.setTokenized(False)
    title.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS)

    text = FieldType()
    text.setIndexed(True)
    text.setStored(True)
    text.setTokenized(True)
    text.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    Page = namedtuple('Page', 'p_id, title, text')
    with open(f, 'rU') as csv_file:
        reader = csv.reader(csv_file)
        count = 0
        for page in map(Page._make, reader):
            try:
                doc = Document()
                doc.add(Field("title", page[1], title))
                doc.add(Field("text", page[2], text))
                writer.addDocument(doc)
                count += 1
            except Exception, e:
                print "Failed in indexDocs:", e
    writer.commit()
    writer.close()
    end = datetime.now()
    print "All done. Indexed {} articles in {}".format(count, end - start)

@begin.start
def run(index_path="/tmp/wiki-lucene-index", *files):
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print 'lucene', lucene.VERSION
    index(os.path.join(index_path, INDEX_DIR), files[0])
