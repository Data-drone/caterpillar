Introduction
============

What is Caterpillar?
--------------------
Caterpillar is a pythonic text indexing and analytics library. While there are quite a few open source libraries
available to perform this job, Caterpillar has support for **qualitative** analysis built-in, not just
**quantitative** analysis. Additionally, Caterpillar can act as a regular information retrieval library just like
Lucene or Whoosh.

In Caterpillar, qualitative analysis is made possible by storing *extra* information about data added to an index.
When you perform qualitative analysis on text you generally need to store extra information about that text. For
example, most qualitative analysis techniques will want a context block smaller then a document like a sentence or a
paragraph. Caterpillar allows you to specify this at an index level and retrieve that information about an index. This
means you can still store *documents* in your index (like Wikipedia articles for example), but also do a qualitative
analysis on your data.

Caterpillar supports the regular quantitative features of in information retrieval engine (counting, filtering etc.).
This means you can combine a filtered query with a qualitative analysis.

Err, What? Example Please...
----------------------------
Lets say you have a CSV file as follows:

.. csv-table:: Survey Responses
   :header: "Gender", "Age", "NPS Score", "Comment"
   :widths: 10, 10, 5, 100

   "M", 24, 8, "The store looked nice and the products are great but the staff can be rude."
   "F", 35, 5, "You don't stock clothes in my size and my size isn't uncommon!"
   "F", 29, 9, "The range of kids clothes is amazing and the prices are very reasonable."

Using Caterpillar, you can:

* Add each row of the CSV as a document
* Ask for certain rows back based on filters - ``gender=f`` or ``gender=f and age < 30`` etc.
* Perform qualitative analysis on all rows or a subset of rows based on a query (like above). An example of a
  qualitative analysis might be to use `LSA to extract topics <https://github.com/Kapiche/caterpillar-lsi>`_ or do a
  sentiment analysis.

Why Caterpillar?
----------------
Caterpillar was originally developed by the guys at `Kapiche <http://kapiche.com>`_ because there wasn't an existing
solution that that could find that met their existing needs or could be easily adapted to meet their needs. Kapiche use
caterpillar to provide a range of analytics products and services. They have written their own proprietary automated
topic extraction and sentiment analysis technologies that the plug-in to Caterpillar.

If you have a desire to store/retrieve data and do both quantitative **AND** qualitative analysis on it, then
Caterpillar might be a good solution for you.

Supported Platforms
-------------------
Most *nix systems. Tested on Ubuntu, Debian and OS X but should work on any *nix system with access to Python 2.7+ and
a C compiler.

Do you support Python 3?
------------------------
No, but support is planned for version 2.0 due early 2015.

