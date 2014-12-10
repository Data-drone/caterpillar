.. _indexes-schemas:

Indexes and Schemas
===================
Indexes store documents. Documents are made up of one or more fields. Each field has a type. Each index has a schema
that specifies what fields the index accepts. Documents added to and index must have one or more field from the schema
for that index. Any unrecognised fields are ignored. Schemas aren't as strict as a relational database schema for
example where you can have required fields. Rather, the only requirement for a document to be added to an index is that
it must have one or more field from the index's schema. There is no notion of a *null* value.

Designing a Schema
------------------
A schema is made up of one or more fields. Fields have a type and some options. All fields share two options:

* ``stored`` - Is this field's value stored with the document? If ``True``, when retrieving documents from an index via
  a search, this fields value will be returned with the document.
* ``indexed`` - Is this field indexed? If ``True``, it's value will be analysed when it is added to the index and
  become searchable. If ``False``, this field **won't** be indexed and won't be able to search for a document using this
  field.

Field Types
-----------
Caterpillar comes with a number of field types. Refer to their documentation for more information:

    * :class:`BOOLEAN <caterpillar.processing.schema.BOOLEAN>`
    * :class:`CATEGORICAL_TEXT <caterpillar.processing.schema.CATEGORICAL_TEXT>`
    * :class:`ID <caterpillar.processing.schema.ID>`
    * :class:`NUMERIC <caterpillar.processing.schema.NUMERIC>`

Example
-------
Lets say you would like to store tweets and make them searchable by the tweet content. However, you are doing an
analysis of tweet timelines rather then the tweet content and your server has space restrictions. In this instance,
while you want to be able to search via the tweet content, but are only interested in the time of the tweet, you can
construct a schema where the tweet content is searchable (indexed) but not stored.

    schema = Schema(
        time=NUMERIC(stored=True, indexed=False)  # Stored but not searchable
        content=TEXT(stored=False, indexed=True)  # Not stored but still searchable
    )

