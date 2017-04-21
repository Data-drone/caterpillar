# Copyright (c) Kapiche Limited
# Author: Sam Hames <sam.hames@kapiche.com>
"""
The schema scripts for the bulk operations of the :class:`.SqliteStorage`.

"""


# Schema for the staging database
cache_schema = """
begin;

create table field (
    name text primary key
);

create table field_setting(
    name text,
    key text,
    value
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
create table stage_posting (
    frame_id integer,
    term text,
    frequency integer,
    positions text,
    primary key(frame_id, term)
);

/* one row per attribute in a frame. */
create table attribute_posting (
    frame_id integer,
    type text,
    value,
    primary key (frame_id, type, value)
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


commit;
begin immediate;

"""

# Prepare commit by precalculating and sorting everything.
# Returns rows representing the current maximum frame and document ID's for assigning
# ID's during the script execution.
prepare_flush = """
-- For generating the term-frame_id and attribute-frame_id tables
create index term_idx on stage_posting(term, frame_id);
create index attribute_idx on attribute_posting(type, value, frame_id);
create table distinct_attributes as
select distinct type, value from attribute_posting;

-- Generate term statistics of added documents
create table term_statistics as
    select
        term,
        field_name,
        sum(frequency) as frequency,
        count(distinct frame_id) as frames_occuring,
        count(distinct document_id) as documents_occuring
    from stage_posting pos
    inner join frame
        on frame_id = frame.id
    group by
        pos.term,
        frame.field_name;

create index term_stats_idx on term_statistics(term, field_name);

commit; -- end staging transaction so we can attach on disk database.

-- Attach the on-disk database to flush to.
attach database ? as disk_index;

begin immediate; -- Begin the true transaction for on disk writing

-- Max document and frame id's at the start of the write process.
select * from index_revision
where revision_number = (select max(revision_number) from index_revision);

-- Actual ID's of deleted documents, for reporting succesful deletion and updating delete counts
select distinct id from deleted_document where id in (select id from disk_index.document);

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
create index deleted_frame_idx on deleted_frame(id);

delete from disk_index.term_posting
-- Avoid full table scan by searching for term_id first
-- This is still going to be expensive.
where term_id in (select distinct term_id
                  from disk_index.frame_posting
                  where frame_id in (select * from deleted_frame))
    and frame_id in (select * from deleted_frame);

delete from disk_index.frame_posting
    where frame_id in (select * from deleted_frame);

delete from disk_index.attribute_frame_posting
-- Avoid full table scan by searching for attribute_id first
-- This is still going to be expensive.
where attribute_id in (
    select distinct attribute_id
    from disk_index.frame_attribute_posting
    where frame_id in (select * from deleted_frame)
)
    and frame_id in (select * from deleted_frame);

delete from disk_index.frame_attribute_posting
    where frame_id in (select * from deleted_frame);

-- Delete all the places the document occurs.
delete from disk_index.document where id in (select * from deleted_document);
delete from disk_index.frame where document_id in (select * from deleted_document);
delete from disk_index.document_data where document_id in (select * from deleted_document);


/* Add new indexed fields

A field can be added either as an addition to the Index schema, or via migration
from an earlier version of the index layout. Duplicates are managed via the
CaterpillarLegacySchema object, so we don't need to worry about them here.

*/
insert or ignore into disk_index.field(name)
    select * from field;

insert into disk_index.field_setting(field_id, key, value)
    select f.id, key, value
    from main.field_setting fs
    inner join disk_index.field f
        on f.name = fs.name;


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
    inner join disk_index.field fields
        on fields.name = data.field_name;


insert into disk_index.frame(id, document_id, field_id, sequence, stored)
    select
        frame.id + :max_frame,
        document_id + :max_doc,
        fields.id,
        sequence,
        stored
    from frame
    inner join disk_index.field fields
        on fields.name = frame.field_name;


/* Term and frame posting data */
insert into disk_index.frame_posting(frame_id, term_id, frequency, positions)
    select
        pos.frame_id + :max_frame,
        vocab.id,
        frequency,
        positions
    from stage_posting pos
    inner join disk_index.vocabulary vocab
        on vocab.term = pos.term;


insert into disk_index.term_posting(term_id, frame_id, frequency, positions)
    select
        vocab.id,
        pos.frame_id + :max_frame,
        frequency,
        positions
    from stage_posting pos
    inner join disk_index.vocabulary vocab
        on vocab.term = pos.term
    order by vocab.id;


/* Insert new attribute-value pairs */
insert into disk_index.attribute(type, value)
    select type, value
    from distinct_attributes attr
    where not exists (
        select 1
        from disk_index.attribute at
        where attr.type = at.type
            and attr.value = at.value
    );

/* Add to the attribute-frame postings indexes

Note that we're making sure the frames actually exist.

*/

insert into disk_index.frame_attribute_posting(frame_id, attribute_id)
    select frame_id, attribute.id
    from attribute_posting post
    inner join disk_index.attribute
        on attribute.type = post.type
        and attribute.value = post.value
    inner join disk_index.frame
        on frame.id = post.frame_id
    order by frame_id, attribute.id;

insert into disk_index.attribute_frame_posting(frame_id, attribute_id)
    select frame_id, attribute.id
    from attribute_posting post
    inner join disk_index.attribute
        on attribute.type = post.type
        and attribute.value = post.value
    inner join disk_index.frame
        on frame.id = post.frame_id
    order by attribute.id, frame_id;


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
    inner join disk_index.field fields
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
insert into index_revision(added_document_count, deleted_document_count, added_frame_count)
    values(:added, :deleted, :added_frames);

-- Update the field statistics
insert or replace into field_statistics(field_id, frame_count)
    select field_id, count(*)
    from disk_index.frame
    group by field_id;

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

-- Return plugin ID's of new and inserted plugins
select plugin_type, settings, plugin_id
from plugin_registry
inner join disk_index.plugin_registry disk_reg
    using (settings, plugin_type);


delete from field;
delete from document;
delete from document_data;
delete from deleted_document;
delete from frame;
delete from setting;
delete from stage_posting;
delete from plugin_data;
delete from plugin_registry;
delete from delete_plugin;
drop table term_statistics;
drop index term_idx;

"""
