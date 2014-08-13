# Copyright (c) 2012-2014 Kapiche Limited
# Aurthor: Ryan Stuart <ryan@kapiche.com>
"""
Read in a bunch of pre-parsed csv files produced from the Wikipedia XML dump (1 article per row) and turn them into an
index.

"""
import cProfile
import csv
import pstats
import sys

import begin
from whoosh.fields import TEXT, Schema, ID
from whoosh.index import create_in, os


@begin.start
@begin.convert(num_of_docs=int, step_size=int, profile=bool)
def run(index_path="/tmp/wiki-whoosh-index", profile=False, *files):
    """
    Parse the Wikipedia csv dumps in ``files`` and save in an index per csv file at ``index_path``.

    """
    schema = Schema(title=TEXT(stored=True), id=ID(stored=True), text=TEXT(stored=True))
    print "Adding all documents from the {:,} Wikipedia csv dumps...Wew!".format(len(files))
    csv.field_size_limit(100000000000000)

    def index(f, index):
        with open(f, 'rU') as csf_file:
            reader = csv.reader(csf_file)
            count = 0
            real_count = 0
            os.makedirs(index)
            ix = create_in(index, schema)
            writer = ix.writer()
            for page in reader:
                real_count += 1
                if not int(page[2]):
                    count += 1
                    writer.add_document(id=unicode(page[0]), title=unicode(page[1], 'utf-8'), text=unicode(page[4], 'utf-8'))
                    if count % 1000 == 0:
                        break
                if real_count % 1000 == 0:
                    print "Parsed {:,} documents so far, ({:,} actual articles), continuing...".\
                        format(real_count, count)
            sys.stdout.write("Parsed all {:,} documents ({:,} actual articles), writing index...".
                             format(real_count, count))
            writer.commit()
            sys.stdout.write("DONE\n")

    def work():
        count = 0
        for f in files:
            index(f, "{}-{}".format(index_path, count))
            count += 1
        print "All done. We created {:,} indexes.".format(count)

    if profile:
        pr = cProfile.Profile()
        pr.runcall(work)
        ps = pstats.Stats(pr)
        ps.dump_stats("storage_benchmark.profile")
    else:
        work()



