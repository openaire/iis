CREATE EXTERNAL TABLE document
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${input_document_authors_citations}'
TBLPROPERTIES ('avro.schema.literal'='${schema_input_document_authors_citations}');

CREATE EXTERNAL TABLE projectId
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${input_project_id}'
TBLPROPERTIES ('avro.schema.literal'='${schema_input_project_id}');

CREATE EXTERNAL TABLE personId
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${input_person_id}'
TBLPROPERTIES ('avro.schema.literal'='${schema_input_person_id}');


CREATE TABLE document_statistics
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output_document_statistics}'
TBLPROPERTIES ('avro.schema.literal'='${schema_output_document_statistics}');

CREATE TABLE author_statistics
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output_author_statistics}'
TBLPROPERTIES ('avro.schema.literal'='${schema_output_author_statistics}');

CREATE TABLE project_statistics
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output_project_statistics}'
TBLPROPERTIES ('avro.schema.literal'='${schema_output_project_statistics}');

CREATE TABLE global_statistics
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output_global_statistics}'
TBLPROPERTIES ('avro.schema.literal'='${schema_output_global_statistics}');

-- UDFs

CREATE TEMPORARY FUNCTION collect_all AS 'eu.dnetlib.iis.core.hive.CollectAllUDAF';
CREATE TEMPORARY FUNCTION count_array AS 'eu.dnetlib.iis.core.hive.CountArrayElementsUDF';
CREATE TEMPORARY FUNCTION empty_array AS 'eu.dnetlib.iis.core.hive.CreateEmptyArrayUDF';
CREATE TEMPORARY FUNCTION merge_maps AS 'eu.dnetlib.iis.core.hive.MergeStringIntMapsUDAF';
CREATE TEMPORARY FUNCTION list_to_map AS 'eu.dnetlib.iis.core.hive.CountArrayElementsUDAF';
CREATE TEMPORARY FUNCTION gen_coauthors AS 'eu.dnetlib.iis.statistics.hive.GenerateCoauthorsUDF';

-- common tables

create table citations
location '${workingDir}/citations'
as
select documentId, year, refDocId, isPublished
from document lateral view explode(referencedDocumentsIds) ids AS refDocId;

create table citationsWithNulls
location '${workingDir}/citationsWithNulls'
as
select citations.documentId as sourceId, citations.isPublished as sourcePublished, citations.year, document.documentId as targetId, document.isPublished as targetPublished from
document left outer join citations
on document.documentId = citations.refDocId;

create table publishedSourceCitations
location '${workingDir}/publishedSourceCitations'
as
select * from citationsWithNulls where sourcePublished or sourceId is null;

create table docStats
location '${workingDir}/docStats'
as
select
    targetId as docId,
    collect_all(targetPublished)[0] as published,
    cast(count(sourceId) as INT) as numberOfCitations,
    count_array(collect_all(year)) as numberOfCitationsPerYear,
    map("1", if(count(sourceId) >= 1, 1, 0),
        "10", if(count(sourceId) >= 10, 1, 0),
        "50", if(count(sourceId) >= 50, 1, 0),
        "100", if(count(sourceId) >= 100, 1, 0),
        "250", if(count(sourceId) >= 250, 1, 0),
        "500", if(count(sourceId) >= 500, 1, 0)) as numberOfPapersCitedAtLeastXTimes
from citationsWithNulls group by targetId;

create table publishedSourceDocStats
location '${workingDir}/publishedSourceDocStats'
as
select
    targetId as docId,
    collect_all(targetPublished)[0] as published,
    cast(count(sourceId) as INT) as numberOfCitations,
    count_array(collect_all(year)) as numberOfCitationsPerYear,
    map("1", if(count(sourceId) >= 1, 1, 0),
        "10", if(count(sourceId) >= 10, 1, 0),
        "50", if(count(sourceId) >= 50, 1, 0),
        "100", if(count(sourceId) >= 100, 1, 0),
        "250", if(count(sourceId) >= 250, 1, 0),
        "500", if(count(sourceId) >= 500, 1, 0)) as numberOfPapersCitedAtLeastXTimes
from publishedSourceCitations group by targetId;

create table allDocStatistics
location '${workingDir}/allDocStatistics'
as
select
    docStats.docId,
    docStats.published,
    docStats.numberOfCitations,
    docStats.numberOfCitationsPerYear,
    docStats.numberOfPapersCitedAtLeastXTimes,
    coalesce(publishedSourceDocStats.numberOfCitations, 0) as numberOfPublishedCitations,
    coalesce(publishedSourceDocStats.numberOfCitationsPerYear, map('unknown', 0)) as numberOfPublishedCitationsPerYear,
    coalesce(publishedSourceDocStats.numberOfPapersCitedAtLeastXTimes, map("1", 0, "10", 0, "50", 0, "100", 0, "250", 0, "500", 0)) as numberOfPapersCitedAtLeastXTimesByPublished
from docStats left outer join publishedSourceDocStats
on docStats.docId = publishedSourceDocStats.docId;

create table publishedDocStatistics
location '${workingDir}/publishedDocStatistics'
as
select * from allDocStatistics
where published;


-- document statistics

insert overwrite table document_statistics
select
    docId as documentId,
    named_struct(
        "citationsFromAllPapers", named_struct(
            "numberOfCitations", numberOfCitations,
            "numberOfCitationsPerYear", numberOfCitationsPerYear),
        "citationsFromPublishedPapers", named_struct(
            "numberOfCitations", numberOfPublishedCitations,
            "numberOfCitationsPerYear", numberOfPublishedCitationsPerYear)
    ) as statistic
from allDocStatistics;


-- global statistics

create table globalAll
location '${workingDir}/globalAll'
as
select
    named_struct(
        'numberOfPapers', cast(count(docId) as INT),
        'citationsFromAllPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', cast(sum(numberOfCitations) as INT),
                'numberOfCitationsPerYear', merge_maps(numberOfCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', cast(avg(numberOfCitations) as FLOAT),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimes)
        ),
        'citationsFromPublishedPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', cast(sum(numberOfPublishedCitations) as INT),
                'numberOfCitationsPerYear', merge_maps(numberOfPublishedCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', cast(avg(numberOfPublishedCitations) as FLOAT),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimesByPublished)
        )
    ) as allPapers
from allDocStatistics;

create table globalPublished
location '${workingDir}/globalPublished'
as
select
    named_struct(
        'numberOfPapers', cast(count(docId) as INT),
        'citationsFromAllPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', cast(sum(numberOfCitations) as INT),
                'numberOfCitationsPerYear', merge_maps(numberOfCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', cast(avg(numberOfCitations) as FLOAT),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimes)
        ),
        'citationsFromPublishedPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', cast(sum(numberOfPublishedCitations) as INT),
                'numberOfCitationsPerYear', merge_maps(numberOfPublishedCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', cast(avg(numberOfPublishedCitations) as FLOAT),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimesByPublished)
        )
    ) as publishedPapers
from publishedDocStatistics;

insert overwrite table global_statistics
select globalAll.allPapers, globalPublished.publishedPapers
from globalAll join globalPublished;


-- project statistics

create table projectDocument
location '${workingDir}/projectDocument'
as
select projectId.id as projId, projDoc.documentId from
projectId left outer join
(select documentId, projectId
from document lateral view explode(projectIds) ids as projectId) projDoc
on projectId.id = projDoc.projectId;

create table projectAll
location '${workingDir}/projectAll'
as
select
projId,
    named_struct(
        'numberOfPapers', cast(count(docId) as INT),
        'citationsFromAllPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimes)
        ),
        'citationsFromPublishedPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfPublishedCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfPublishedCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfPublishedCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimesByPublished)
        )
    ) as allPapers
from
(select * from
projectDocument left outer join allDocStatistics
on projectDocument.documentId = allDocStatistics.docId) merged
group by projId;

create table projectPublished
location '${workingDir}/projectPublished'
as
select
projId,
named_struct(
        'numberOfPapers', cast(count(docId) as INT),
        'citationsFromAllPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimes)
        ),
        'citationsFromPublishedPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfPublishedCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfPublishedCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfPublishedCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimesByPublished)
        )
    ) as publishedPapers
from
(select * from
projectDocument left outer join publishedDocStatistics
on projectDocument.documentId = publishedDocStatistics.docId) merged
group by projId;

insert overwrite table project_statistics
select
    projectAll.projId as projectId,
    named_struct(
        'allPapers', projectAll.allPapers,
        'publishedPapers', projectPublished.publishedPapers
    ) as statistic
from projectAll join projectPublished
on projectAll.projId = projectPublished.projId;


-- author stats

create table authorDocument
location '${workingDir}/authorDocument'
as
select personId.id as authorId, authDoc.documentId from
personId left outer join
(select documentId, authorId
from document lateral view explode(authorIds) ids as authorId) authDoc
on personId.id = authDoc.authorId;

create table authorAll
location '${workingDir}/authorAll'
as
select
authorId,
named_struct(
        'numberOfPapers', cast(count(docId) as INT),
        'citationsFromAllPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimes)
        ),
        'citationsFromPublishedPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfPublishedCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfPublishedCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfPublishedCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimesByPublished)
        )
    ) as allPapers
from
(select * from
authorDocument left outer join allDocStatistics
on authorDocument.documentId = allDocStatistics.docId) merged
group by authorId;

create table authorPublished
location '${workingDir}/authorPublished'
as
select
authorId,
named_struct(
        'numberOfPapers', cast(count(docId) as INT),
        'citationsFromAllPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimes)
        ),
        'citationsFromPublishedPapers',
        named_struct(
            'basic',
            named_struct(
                'numberOfCitations', coalesce(cast(sum(numberOfPublishedCitations) as INT), 0),
                'numberOfCitationsPerYear', merge_maps(numberOfPublishedCitationsPerYear)
            ),
            'averageNumberOfCitationsPerPaper', coalesce(cast(avg(numberOfPublishedCitations) as FLOAT), 0),
            'numberOfPapersCitedAtLeastXTimes', merge_maps(numberOfPapersCitedAtLeastXTimesByPublished)
        )
    ) as publishedPapers
from
(select * from
authorDocument left outer join publishedDocStatistics
on authorDocument.documentId = publishedDocStatistics.docId) merged
group by authorId;

create table docCoauthors
location '${workingDir}/docCoauthors'
as
select documentId, authorId, authorIds
from document lateral view explode(authorIds) ids as authorId;

create table coAuthorsMap
location '${workingDir}/coauthorstmp'
as
select authorId, list_to_map(authorIds) as coAuthorsMap
from docCoauthors
group by authorId;

create table coauthors
location '${workingDir}/coauthors'
as
select authorId, gen_coauthors(authorId, coAuthorsMap) as coAuthors
from coAuthorsMap;


create table coauthorsFull
location '${workingDir}/coauthorsFull'
as
select personId.id as authorId, coalesce(coauthors.coAuthors, empty_array(named_struct("id", "id", "coauthoredPapersCount", 0))) as coAuthor from
personId left outer join coauthors
on personId.id = coauthors.authorId;

insert overwrite table author_statistics
select
    authorAll.authorId, named_struct(
        "core", named_struct(
            'allPapers', authorAll.allPapers,
            'publishedPapers', authorPublished.publishedPapers),
        "coAuthors", coauthorsFull.coAuthor
    ) as statistic
from authorAll join authorPublished
on authorAll.authorId = authorPublished.authorId
join coauthorsFull on authorPublished.authorId = coauthorsFull.authorId;
