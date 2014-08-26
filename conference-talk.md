Title
=====
Big Data and Python: Lessons Learned

Category
========
Science

Duration
========
No Preference

Description
===========
Python is gaining momentum as the preferred tool for data science and experimentation. This is largely due to powerful libraries like Gensim, Pandas, etc. that leverage Python's ease of use. But can we use Python in an enterprise-oriented Big Data context? We have attempted to do exactly that with our Python text indexer Caterpillar, and following are some of the lessons we learned along the way.

Audience
========
People interested in using a Python-based solution to Big Data problems; anyone interested in techniques to improve Python performance with Big Data.

Python Level
============
Intermediate.

Objective
=========
Share our experience with writing an open source Python text indexer intended to be comparable in speed to Lucene and used in commercial Big Data solutions. This will include why we decided to write our own text indexer, why we chose Python, our approach to the Python 2 vs Python 3 struggle and techniques we learned along the way to make our software run faster. Attendees who haven't heard of Caterpillar will also leave with a basic understanding of what Caterpillar is and how it can be used.

Detailed Abstract
=================
At the start of 2013, we were eager to start our own text analytics company (an industry we already had extensive experience in). We envisaged a highly-flexible piece of software that our customers could use to both store their data and explore it using quantitative and/or qualitative techniques (think Elasticsearch + Gensim). We faced a choice: do we mix together existing technologies, add some of our own code to get a workable solution, or do we start from scratch? Existing technologies like [Lucene][1] (Java), [Whoosh][2] (Python),  [Gensim][3] (Python) and [Elasticsearch][4] (Java) etc. were available, but how much work is involved in adapting them to our needs? Is Java a place we want to be? We thought we managed to escape Java!

**Our Solution**
Ultimately, we decided to write our own library. [Caterpillar][5] is an open source text indexing library that supports custom analytics as plug-ins. Why? Because all of the other solutions were either too far removed from what we needed or, as with some of the Java libraries, had a code base that quickly reminded us why we turned our back on the Java world for greener pastures. We made this decision despite the stigma of Python being too slow for enterprise solutions – a sentiment that has been exposed as a falsehood in our experiences.

**What is Caterpillar**
[Caterpillar][5] is a Python information retrieval and analysis engine. It supports information retrieval in the traditional Lucene/Whoosh sense - documents can be added to, and queried from an index. More interestingly, it allows arbitrary custom analytics to be executed across indexed data via its plug-in framework. It is provided with a [Latent Semantic Indexing plug-in][6] and we also use the engine internally at [Kapiche][7] to run our own proprietary topic modelling algorithm. It can use any key/value store as its storage mechanism. Right now, it uses Sqlite3.

**Python 2 vs Python 3**
First question when writing a new library, which version of Python? Well, like almost everyone else (we suspect), the answer to this was dictated at the start by our dependencies. We began with NLTK as a dependency in early 2013. Back then, NLTK didn't support Python 3. Now, we have removed NLTK as a dependency and the features in Python 3 (both syntax and standard library improvements) are too good to turn down. So, the question for us becomes do we continue to support Python 2? We suspect if we started now, we wouldn't support Python 2.

**Speed**
Is it fast enough? Short answer is yes, we are faster then Whoosh and comparable to Lucene. The long answer is in the benchmarks we will present. There are a number of techniques we utilised for our current performance standards and we expect significant gains if we decide to move components over to C extensions (all our code is in Python for the time being although some of our dependencies use C extensions). Some core facets of our performance strategy include:

 - A segmented index design inspired by Lucene and used by Whoosh.
 - The use of the fastest key/value store we could find.
 - Efficient use of regular expressions (and removing NLTK which is a fantastic tool but not written for speed).
 - Minimise if..else, favour try...except or defaultdict.
 - Don't loop and calculate, itertools is your friend.
 - Generators are awesome, use them (more memory-orientated than speed).

We will demonstrate the efficacy of these strategies via our benchmarks. The number one lesson we learned - and if you take anything away from this talk, let it be this - you need to profile your code! timeit, profile/cProfile/line-profiler and heapy/memory-profiler (plus others) are all your friend!

**Future**
Where are we headed with Caterpillar? Right now, our biggest bottlenecks are the speed to index a document (requires the computation of some big data structures and some often complicated tokenisation) and memory usage. Things can never run fast enough – performance utopia is a constantly evolving target. Only time will reveal whether it is achieved through C-extensions/Cython or even more specialised data structures.

  [1]: http://lucene.apache.org/core/
  [2]: https://bitbucket.org/mchaput/whoosh/wiki/Home
  [3]: http://radimrehurek.com/gensim/
  [4]: http://www.elasticsearch.org/
  [5]: https://github.com/Kapiche/caterpillar
  [6]: https://github.com/Kapiche/caterpillar-lsi
  [7]: http://kapiche.com/

Outline
=======
**INTRO (8 mins)**

 - Who are we?
 - What is Caterpillar?
 - Why Caterpillar?

**Speed (10 mins)**

 - How fast is Caterpillar (benchmarks)?
 - How did we get it this fast?
 - How can we get it faster?

**Python 2 vs Python 3 (5 mins)**

 - Which one should you choose?
 - Which we did we choose first and why?
 - What we are using now and why

**The Future (5 mins)**

 - What's next for Caterpillar?
 - Contributors welcome!

**Questions (2 mins)**

Additional Notes
================
We are both comfortable with public speaking and have attended many previous PyConAU conferences as well as Linux Conference Australia conferences. Ryan is on the organising committee for PyConAU 2014 & 2015.

The development of Caterpillar is part of our full-time job.  While we certainly aren't advocating conference-driven development, the core Caterpillar code will change considerably between now and the conference date and the content of the talk will need to updated accordingly. In particular, we are in the middle of making Caterpillar Python 3 compatible. We have yet to decide if we will support Python 2 at all, and the rationale around this decision is something we would like to incorporate.

Additional Requirements
=======================
Nil.
