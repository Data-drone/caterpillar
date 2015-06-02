What is Caterpillar?
====================

.. image:: https://img.shields.io/travis/Kapiche/caterpillar.svg?style=flat-square
    :target: https://travis-ci.org/Kapiche/caterpillar
.. image:: https://img.shields.io/coveralls/Kapiche/caterpillar.svg?style=flat-square
    :target: https://coveralls.io/r/Kapiche/caterpillar
.. image:: https://codeship.com/projects/YOUR_PROJECT_UUID/status?branch=master

Caterpillar is a pure python text indexing and analytics library. Some features include:

* pluggable key/value object store for storage (currently only implementation is SQLite)
* transaction layer for reading/writing (along with associated locking semantics)
* supports searching indexes with some built in scoring algorithm implementations (including TF/IDF)
* stores additional data structures for analytics above and beyond traditional information retrieval data structures
* has a plugin architecture for quickly accessing the data structures and performing custom analytics
* has 100% test coverage


Quick Example
=============
Quick example of using caterpillar below::

    import os
    import tempfile
    
    from caterpillar.processing.index import IndexWriter, IndexConfig
    from caterpillar.processing.schema import TEXT, Schema, NUMERIC
    from caterpillar.storage.sqlite import SqliteStorage
    
    index_dir = os.path.join(tempfile.mkdtemp(), "examples")
    with open('caterpillar/test_resources/moby.txt', 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT, some_number=NUMERIC))) as writer:
            writer.add_document(text=data, some_number=1)
    
Installation
============
.. code::

    pip install caterpillar

Documentation
=============
The documentation can be found `here <http://caterpillar.readthedocs.org/en/latest/>`_.
    
Roadmap
=======
We are working on porting our issues from our internal issue tracker over to a more visible system. But, for the time
being, here is a general roadmap:

* Move to (possibly only) Python 3 (see below).
* Revamp schema and field design.
* Add a memory storage implementation.
* Revamp query design.
* Remove the NLTK dependency (great library, but only used for tokenisation).
* Switch index structures over to a more efficient data structure (possibly numpy arrays or similar).
    
The current plan is to move to using GitHub issues with HuBoard, but stay tuned.
    
Python Version
==============
Currently Python 2.7+ only. Work is underway to support Python 3+. **WARNING**: Caterpillar *might* become Python 3+ 
**only** in the future. Stay tuned.

BDFLs
=====
* `Kris Rogers <https://github.com/krisrogers/>`_
* `Ryan Stuart <https://github.com/rstuart85/>`_

Contributors
============
Anyone who is willing! In other words none yet, but we are more then accepting of contributions.

Contributing
============
Not code will be merged unless it has 100% test coverage and passes pep8. We code with a line length of 120 characters 
(see tox.ini [pep8] section) and we use `py.test <http://pytest.org/>`_ for testing. Tests are in a *test* sub-folder in 
each package. We generally run coverage as follows::

    coverage erase; coverage run --source caterpillar -m py.test -v caterpillar; coverage report

Copyright and License
=====================
Caterpillar is copyright © 2013 - 2015 Kapiche Limited. It is licensed under the GNU Affero General Public License.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

The copyright holders grant you an additional permission under Section 7 of the GNU Affero General Public License, version 3, exempting you from the requirement in Section 6 of the GNU General Public License, version 3, to accompany Corresponding Source with Installation Information for the Program or any work based on the Program. You are still required to comply with all other Section 6 requirements to provide Corresponding Source.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
