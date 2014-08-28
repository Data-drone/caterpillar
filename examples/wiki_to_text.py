# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import csv
import begin
from mw.xml_dump import Iterator
from mw.xml_dump.element_iterator import ElementIterator


@begin.start
@begin.convert(num_of_articles=int, step_size=int)
def run(xml_dump, output_file, num_of_articles=0, step_size=10000):
    el = ElementIterator.from_file(open(xml_dump))
    dump = Iterator.from_element(el)
    file_count = 1
    count = 0
    real_count = 0
    try:
        while True:
            path = "{}-{:,}-{:,}.csv".format(output_file, step_size*(file_count - 1) + 1, step_size*file_count)
            with open(path, 'w') as f:
                for _ in xrange(step_size):
                    writer = csv.writer(f)
                    page = next(dump)
                    real_count += 1
                    if page.redirect is None:
                        count += 1
                        revision = next(page)
                        if revision.text:
                            row = [page.id, page.title, 0, revision.id, revision.text]
                        else:
                            row = [page.id, page.title, 0, revision.id, ""]
                    else:
                        row = [page.id, page.title, 1, '', '']
                    writer.writerow(row)
                    if real_count % step_size == 0:
                        print "Written out {:,} articles so far, {:,} real ({}).".format(real_count, count, path)
            if num_of_articles and real_count >= num_of_articles:
                break
            file_count += 1
    except StopIteration:
        pass
    finally:
        print "Wrote out {:,} articles, {:,} real and {:,} redirects.".format(real_count, count, real_count-count)
