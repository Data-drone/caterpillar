# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import os
import sys

import lucene
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.util import Version


INDEX_DIR = "IndexFiles.index"


def run(searcher, analyzer):
    while True:
        print
        print "Hit enter with no input to quit."
        command = raw_input("Query:")
        if command == '':
            return

        print
        print "Searching for:", command
        query = QueryParser(Version.LUCENE_CURRENT, "text", analyzer).parse(command)
        scoreDocs = searcher.search(query, 50).scoreDocs
        print "%s total matching documents." % len(scoreDocs)

        for scoreDoc in scoreDocs:
            doc = searcher.doc(scoreDoc.doc)
            print 'title:', doc.get("title"), 'text:', doc.get("text")


if __name__ == '__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print 'lucene', lucene.VERSION
    directory = SimpleFSDirectory(File(os.path.join("/tmp/wiki-lucene-index", INDEX_DIR)))
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
    run(searcher, analyzer)
    del searcher
