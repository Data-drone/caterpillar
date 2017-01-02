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

Currently this table allows repeating terms to support destructive merge variants and
case folding. In the future this table will become append only and term uniqueness will
be enforced, and an alternate mechanism for specifying vocabulary variations will be provided.
*/
create table vocabulary (
    id integer primary key,
    term text
);
create index term_idx on vocabulary(term);


/* Summary statistics for a given term_id by field.

This allows direct lookups for Tf.IDF searches and similar.
*/
create table term_statistics (
    term_id integer,
    field_id integer,
    frequency integer,
    frames_occuring integer,
    documents_occuring integer,
    primary key(term_id, field_id),
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
- primary design purpose is for returning ordered lists of document ID's
- takes advantage of SQLite's permissive type system

*/
create table document_data (
    field_id integer,
    value,
    document_id integer,
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
    positions text, -- Ugly hack to allow compatible bigram merging.
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
    positions text,
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
    constraint unique_plugin unique (plugin_type, settings) on conflict ignore
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

The revision number is incremented by one whenever a write transaction adds or deletes documents
from an index.
*/
create table index_revision (
    revision_number integer primary key,
    added_document_count integer,
    deleted_document_count integer,
    constraint unique_revision unique(added_document_count, deleted_document_count) on conflict replace
);

insert into index_revision values (0, 0, 0);


create table setting (
    name text primary key on conflict replace,
    value
);

/* A convenience view for writing queries. */
create view term_search as
    select vocabulary.term, frame.id
    from term_posting
    inner join vocabulary
        on term_posting.term_id = vocabulary.id
    inner join frame
        on term_posting.frame_id = frame.id
    inner join document
        on frame.document_id = document.id;

commit;

"""

"""

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

create table deleted_document(
    id integer primary key
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

/* One row per term occuring in a frame */
create table positions_staging (
    frame_id integer,
    term text,
    frequency integer,
    positions text,
    primary key(frame_id, term)
);

create table setting(
    name text primary key on conflict replace,
    value
);

/* Plugin header and data tables. */
create table plugin_registry (
    plugin_type text,
    settings text,
    constraint unique_plugin unique (plugin_type, settings) on conflict ignore
);

create table plugin_data (
    plugin_type text,
    settings text,
    key text,
    value text,
    primary key(plugin_type, settings, key) on conflict replace
);

create table delete_plugin (
    plugin_type text,
    settings text
);

create table term_merging (
    term_id,
    frame_id,
    frequency,
    positions,
    primary key(term_id, frame_id)
);

create table bigram_staging (
    frame_id,
    term_id,
    left_term,
    left_positions,
    left_frequency,
    right_term,
    right_positions,
    right_frequency,
    primary key(term_id, frame_id)
);

create table bigram_merging (
    term_id,
    frame_id,
    frequency,
    positions,
    primary key(term_id, frame_id)
);


commit;
begin immediate;

"""

# Prepare commit by precalculating and sorting everything.
# Returns rows representing the current maximum frame and document ID's for assigning
# ID's during the script execution.
prepare_flush = """
-- For generating the Term-frame_id table
create index term_idx on positions_staging(term);

-- Generate term statistics of added documents
create table term_statistics as
    select
        term,
        field_name,
        sum(frequency) as frequency,
        count(distinct frame_id) as frames_occuring,
        count(distinct document_id) as documents_occuring
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
select coalesce(max(id), 0) from disk_index.document;
select coalesce(max(id), 0) from disk_index.frame;
select deleted_document_count from index_revision
where revision_number = (select max(revision_number) from index_revision);

"""

# Flush changes from the cache to the index
flush_cache = """
/* Delete documents */
-- Cache term statistics for deleted documents, note the negative for later.
create table deleted_term_statistics as
    select
        term_id,
        field_id,
        -sum(frequency) as frequency,
        -count(distinct frame_id) as frames_occuring,
        -count(distinct document_id) as documents_occuring
    from disk_index.frame_posting post
    inner join disk_index.frame frame
        on frame_id = frame.id
    where frame.document_id in (select * from deleted_document)
    group by
        post.term_id,
        frame.field_id;

create table deleted_frame as
    select id
    from disk_index.frame
    where document_id in (select * from deleted_document);

delete from disk_index.term_posting
-- Avoid full table scan by searching for term_id first
-- This is still going to be expensive.
where term_id in (select distinct term_id
                  from disk_index.frame_posting
                  where frame_id in (select * from deleted_frame))
    and frame_id in (select * from deleted_frame);

delete from disk_index.frame_posting
    where frame_id in (select * from deleted_frame);

-- Delete all the places the document occurs.
delete from disk_index.document where id in (select * from deleted_document);
delete from disk_index.frame where document_id in (select * from deleted_document);
delete from disk_index.document_data where document_id in (select * from deleted_document);


/* Add new indexed fields */
insert into disk_index.structured_field(name)
    select * from structured_field;
insert into disk_index.unstructured_field(name)
    select * from unstructured_field;


/* Update vocabulary with new terms. Insert highest frequency first. */
insert into disk_index.vocabulary(term)
    select term
    from term_statistics stats
    where not exists (
        select 1
        from vocabulary v
        where v.term = stats.term
    )
    group by term
    order by sum(frequency) desc;


/* Insert document and frame data */
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
    inner join disk_index.unstructured_field fields
        on fields.name = frame.field_name;


/* Term and frame posting data */
insert into disk_index.frame_posting(frame_id, term_id, frequency, positions)
    select
        pos.frame_id + :max_frame,
        vocab.id,
        frequency,
        positions
    from positions_staging pos-- TODO: Rename this table in both the schema and the cache
    inner join disk_index.vocabulary vocab
        on vocab.term = pos.term;


insert into disk_index.term_posting(term_id, frame_id, frequency, positions)
    select
        vocab.id,
        pos.frame_id + :max_frame,
        frequency,
        positions
    from positions_staging pos-- TODO: Rename this table in both the schema and the cache
    inner join disk_index.vocabulary vocab
        on vocab.term = pos.term
    order by vocab.id;


/* Update the statistics by combining on-disk, deleted and new values */
with update_stat as (
    select
        v.id as term_id,
        fields.id as field_id,
        frequency,
        frames_occuring,
        documents_occuring
    from main.term_statistics stats
    inner join disk_index.vocabulary v
        on v.term = stats.term
    inner join disk_index.unstructured_field fields
        on fields.name = stats.field_name

    union all

    select *
    from deleted_term_statistics
),
updated_term as (
select distinct term_id, field_id from update_stat
)
insert or replace into disk_index.term_statistics
    select
        term_id,
        field_id,
        sum(frequency),
        sum(frames_occuring),
        sum(documents_occuring)
    from (select *
          from update_stat
          union all
          select *
          from disk_index.term_statistics
          where term_id in (select term_id from updated_term)
            and field_id in (select field_id from updated_term)
    )
    group by term_id, field_id;


-- Update settings
insert into disk_index.setting
    select *
    from setting;

-- Update the revision number of the database
insert or replace into index_revision(added_document_count, deleted_document_count) values(:added, :deleted);

-- Update the plugins
create table delete_plugin_id as
    select registry.plugin_id
    from disk_index.plugin_registry registry
    inner join delete_plugin del
        on del.plugin_type = registry.plugin_type
        -- If settings is not supplied, delete all plugins of that type
        and (del.settings = registry.settings or del.settings is NULL)
;

delete from disk_index.plugin_data
where plugin_id in (select * from delete_plugin_id);

delete from disk_index.plugin_registry
where plugin_id in (select * from delete_plugin_id);

-- Insert header record for plugins
insert into disk_index.plugin_registry(plugin_type, settings)
    select *
    from plugin_registry;

-- Clear old data for updated plugins and insert new data
delete from disk_index.plugin_data
    where plugin_id in (
        select plugin_id
        from plugin_registry
        inner join disk_index.plugin_registry
            using(plugin_type, settings)
        );

insert into disk_index.plugin_data
    select plugin_id, plugin_data.key, plugin_data.value
    from plugin_data
    inner join disk_index.plugin_registry
        using(plugin_type, settings)
;

delete from structured_field;
delete from unstructured_field;
delete from document;
delete from document_data;
delete from deleted_document;
delete from frame;
delete from setting;
delete from positions_staging;
delete from plugin_data;
delete from plugin_registry;
delete from delete_plugin;
drop table term_statistics;
drop index term_idx;

"""
