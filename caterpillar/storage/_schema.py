# Copyright (c) Kapiche Limited
# Author: Sam Hames <sam.hames@kapiche.com>
"""
The schema scripts for the bulk operations of the :class:`.SqliteStorage`.

"""

disk_schema = """
pragma journal_mode = WAL;
pragma page_size = 4096; -- current recommended value for SQLite.

begin;

/* Field names and ID's

Structured and unstructured fields are kept separate because the storage and querying
of each type of data is different.
*/
create table structured_field (
    id integer primary key,
    name text unique
);

create table unstructured_field (
    id integer primary key,
    name text unique
);

/*
The core vocabulary table assigns an integer ID to every unique term in the index.

Joins against the core posting tables are always integer-integer and in sorted order.
*/
create table vocabulary (
    id integer primary key,
    term text unique
);


/* Summary statistics for a given term_id by field.

This allows direct lookups for Tf.IDF searches and similar.
*/
create table term_statistics (
    term_id integer,
    field_id integer,
    frequency integer,
    frames_occuring integer,
    documents_occuring integer,
    primary key(term_id, field_id) on conflict replace,
    foreign key(term_id) references term(id),
    foreign key(field_id) references field(id)
);


/* The source table for the document representation. */
create table document (
    id integer primary key,
    stored text -- This should be a text serialised representation of the document, such as JSON.
);

/*
Storage for 'indexed' structured fields in the schema.

- designed for sparse data and extensible schema's
- primary design purpose is for returning lists of document ID's
- takes advantage of SQLite's permissive type system

*/
create table document_data (
    document_id integer,
    field_id integer,
    value,
    primary key(field_id, value, document_id),
    foreign key(document_id) references document(id),
    foreign key(field_id) references structured_field(id)
);


create table frame (
    id integer primary key,
    document_id integer,
    field_id integer,
    sequence integer,
    stored text -- The stored representation of the frame.
);


/* Index to access by document ID

Bridges between:
structured data searches --> frames
unstructured searches --> documents
*/
create index document_frame_bridge on frame(document_id, field_id);


/* Postings organised by term, allowing search operations. */
create table term_posting (
    term_id integer,
    frame_id integer,
    frequency integer,
    primary key(term_id, frame_id),
    foreign key(term_id) references term(id),
    foreign key(frame_id) references frame(id)
)
without rowid; -- Ensures that the data in the base table is kept in this sorted order.

/* Postings organised by frame, for term-frequency vector representations of documents and frames. */
create table frame_posting (
    frame_id integer,
    term_id integer,
    frequency integer,
    primary key(frame_id, term_id),
    foreign key(term_id) references term(id) on delete cascade
    foreign key(frame_id) references frame(id) on delete cascade
)
without rowid;


/* Plugin header and data tables. */
create table plugin_registry (
    plugin_type text,
    settings text,
    plugin_id integer primary key,
    constraint unique_plugin unique (plugin_type, settings) on conflict replace
);

create table plugin_data (
    plugin_id integer,
    key text,
    value text,
    primary key(plugin_id, key) on conflict replace,
    foreign key(plugin_id) references plugin_registry(plugin_id) on delete cascade
);

/*
An internal representation of the state of the index documents.

Each count is incremented by one when a document is added or deleted. Both numbers are
monotonically increasing and the system is serialised: these numbers can be used to represent
the current state of the system, and can be used to measure some degree of change between
different versions.

For example, if a plugin was run at revision (100, 4), and the current state of the index is
(200, 50), then there is a significant difference between the corpus at the time the plugin
was run and now.
*/
create table index_revision (
    revision_number integer primary key,
    added_document_count integer,
    deleted_document_count integer
);

insert into index_revision values (0, 0, 0);

commit;

"""

"""/* A whitelist of vocabulary variant columns in the vocabulary table.

When a variation is first registered a column is created in the table for that name.
*/
create table vocabulary_variant(
    id integer primary key,
    name text
);

/* Document_id's for soft deletion.

Wherever possible the document content is deleted, but it is not always possible to do so
efficiently.
*/
create table deleted_document (
    document_id integer primary key
);


/*
An internal representation of the state of the index documents.

Each count is incremented by one when a document is added or deleted. Both numbers are
monotonically increasing and the system is serialised: these numbers can be used to represent
the current state of the system, and can be used to measure some degree of change between
different versions.

For example, if a plugin was run at revision (100, 4), and the current state of the index is
(200, 50), then there is a significant difference between the corpus at the time the plugin
was run and now.
*/


/*
A convenience view to simplify writing search queries.

If we move to a segmented or otherwise optimised index structure this view will
combine the tables, so queries should use this in preference to direct table references.

Note that this view uses the canonical representation of the term to represent variants.
*/

create view search_posting as (
    select *
    from term_posting
    inner join vocabulary
        on term_posting.term_id = vocabulary.id
    inner join frame
        on term_posting.frame_id = frame.id
    inner join field
        on frame.field_id = field.id
    where document_id not in (select document_id from deleted_documents)
);
"""

# Schema for the staging database
cache_schema = """
begin;

create table structured_field (
    name text primary key
);
create table unstructured_field (
    name text primary key
);

/* The source table for the document representation. */
create table document (
    id integer primary key,
    stored text -- This should be a text serialised representation of the document, such as JSON.
);

/* Storage for 'indexed' structured fields in the schema. */
create table document_data (
    document_id integer,
    field_name text,
    value,
    primary key(document_id, field_name)
);

create table frame (
    id integer primary key,
    document_id integer,
    field_name text,
    sequence integer, -- The sequence number of the frame in that field of the document
    stored text -- The stored representation of the frame
);

/* One row per occurence of a term in a frame */
create table positions_staging (
    frame_id integer,
    term text,
    frequency integer,
    primary key(frame_id, term)
);

commit;
"""

# Prepare commit by precalculating and sorting everything.
# Returns rows representing the current maximum frame and document ID's for assigning
# ID's during the script execution.
prepare_flush = """
-- For generating the Term-frame_id table
create index term_idx on positions_staging(term);

-- Generate statistics of deleted documents for removal
create table term_statistics as
select
    term,
    field_name,
    sum(frequency) as frequency,
    count(distinct document_id) as documents_occuring,
    count(distinct frame_id) as frames_occuring
from positions_staging pos
inner join frame
    on frame_id = frame.id
group by
    pos.term,
    frame.field_name;

create index term_stats_idx on term_statistics(term, field_name);

commit; -- end staging transaction so we can attach on disk database.

-- Attach the on disk database to flush to.
attach database ? as disk_index;

begin immediate; -- Begin the true transaction for on disk writing

-- Max document and frame id's at the start of the write process.
select coalesce(max(id), 0) from document;
select coalesce(max(id), 0) from frame;

"""

# Flush changes from the cache to the index
flush_cache = """
insert into disk_index.structured_field(name)
    select * from structured_field;
insert into disk_index.unstructured_field(name)
    select * from unstructured_field;


-- Update vocabulary with new terms:
insert into disk_index.vocabulary(term)
    select distinct
        term
    from term_statistics stats
    where not exists (
        select 1
        from vocabulary v
        where v.term = stats.term
    );


insert into disk_index.document(id, stored)
    select
        id + :max_doc,
        stored
    from document;


insert into disk_index.document_data(document_id, field_id, value)
    select
        document_id + :max_doc,
        fields.id,
        value
    from document_data data
    inner join disk_index.structured_field fields
        on fields.name = data.field_name;


insert into disk_index.frame(id, document_id, field_id, sequence, stored)
    select
        frame.id + :max_frame,
        document_id + :max_doc,
        fields.id,
        sequence,
        stored
    from frame
    inner join disk_index.structured_field fields
        on fields.name = frame.field_name;


insert into disk_index.frame_posting(frame_id, term_id, frequency)
    select
        pos.frame_id + :max_frame,
        vocab.id,
        frequency
    from positions_staging pos-- TODO: Rename this table in both the schema and the cache
    inner join disk_index.vocabulary vocab
        on vocab.term = pos.term;


insert into disk_index.term_posting(term_id, frame_id, frequency)
    select
        vocab.id,
        pos.frame_id + :max_frame,
        frequency
    from positions_staging pos-- TODO: Rename this table in both the schema and the cache
    inner join disk_index.vocabulary vocab
        on vocab.term = pos.term
    order by vocab.id;


-- Delete the documents
    -- Delete documents
    -- delete frames
    -- Add a tombstone for that document_id
-- Update the statistics
-- Update the plugins

commit;
detach database disk_index;

"""
