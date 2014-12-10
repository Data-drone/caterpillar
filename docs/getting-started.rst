Getting Started
===============

Installation
------------
For now, you need to install Caterpillar `from source <https://github.com/Kapiche/caterpillar/>`_.
::

    pip install -e https://github.com/Kapiche/caterpillar/#egg=caterpillar

We Have a space reserved on PyPi and version 0.1 will be added to PyPi shortly.

Basic Concepts
--------------
Caterpillar deals primarily in **documents**. A document is made up of 1 or more **fields**. Documents are stored in an
**index**. Adding a document to an index is referred to as **indexing**. An index has a **schema** which describes the
structure of documents stored in the index. When a document is indexed, each field of the document is **analysed** to
retrieve it's value. Each field has a **type**. All types besides TEXT are **categorical** types. A TEXT field performs
tokenisation of its value to break it up into small blocks. A categorical field uses its whole value without any
tokenisation.

When a TEXT field is tokenised, it is broken up into **frames**. The size of a frame is measured in sentences.
Once broken into frames it is then further tokenised into words. How this process occurs is down to the **analyser**
used. Some common tasks performed by an analyser include removing stopwords & detecting compound names (like *Burger
King* for example).

Each field has one or more attributes which control how they are treated by Caterpillar. A field which is **indexed**
becomes searchable/usable in a query. A field which is **stored** is stored in the index. It is possible to have
a field that is **indexed** but not **stored**. That means you can use that field in a query but the field won't
be in the result documents. Likewise, you can have a field that is **stored** but not **indexed**. That field isn't
searchable but will be returned in the result documents.

Once some documents have been added to the index you can then start to request statistics from and search the index.

Code Examples
-------------
The source code has a bunch of examples included. You can
`view them in the examples folder on github <https://github.com/Kapiche/caterpillar/>`_.

Creating and writing an Index
-----------------------------
An index is used to store documents which are comprised of one or more fields. The schema of an index controls what
document fields are support by this index. Field don't have to have a value. Here is some example code creating an
index and adding a document to it.
::

    import os
    import shutil
    import tempfile

    from caterpillar.processing.index import IndexWriter, IndexConfig
    from caterpillar.processing.schema import TEXT, Schema, NUMERIC
    from caterpillar.storage.sqlite import SqliteStorage

    path = tempfile.mkdtemp()
    try:
        index_dir = os.path.join(path, "examples")
        with open('caterpillar/test_resources/moby.txt', 'r') as f:
            data = f.read()
            with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT, some_number=NUMERIC))) as writer:
                writer.add_document(text=data, some_number=1)
    finally:
        shutil.rmtree(path)

Read more about :class:`IndexWriter <caterpillar.processing.index.IndexWriter>`.

Searching an Index
------------------
Once data is stored on an index, you can then retrieve it.
::

    import os
    import shutil
    import tempfile

    from caterpillar.processing.index import IndexWriter, IndexConfig, IndexReader
    from caterpillar.processing.schema import TEXT, Schema
    from caterpillar.searching.query.querystring import QueryStringQuery
    from caterpillar.storage.sqlite import SqliteStorage

    path = tempfile.mkdtemp()
    try:
    index_dir = os.path.join(path + "examples")
    with open('caterpillar/test_resources/alice.txt', 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(fold_case=True, text=data)

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            results = searcher.search(QueryStringQuery('W*e R?bbit and (thought or little^1.5)'))
            print "Retrieved {} of {} matches".format(len(results), results.num_matches)
            for result in results:
                print "Doc ID: {} ; Text: {} ;\n".format(result.doc_id, result.data['text'])
    finally:
        shutil.rmtree(path)

Read more about :class:`IndexSearcher <caterpillar.searching.IndexSearcher>`. See also the section on
:ref:`indexes-schemas`.

Index Storage Formats
---------------------
Caterpillar is designed to work with any key-value store. Interaction with those stores is abstracted via the
:class:`Storage <caterpillar.storage.Storage>` class. Currently the only implementation is
:class:`SqliteStorage <caterpillar.storage.sqlite.SqliteStorage>`.

Querying
--------
Caterpillar has batteries included when it comes to searching. It contains an implementation of TF-IDF
