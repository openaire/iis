CREATE EXTERNAL TABLE person
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${input_person}'
TBLPROPERTIES ('avro.schema.url'='${schema_input_person}');

CREATE EXTERNAL TABLE document
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${input_document}'
TBLPROPERTIES ('avro.schema.url'='${schema_input_document}');

CREATE TABLE person_age
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output_person_age}'
TBLPROPERTIES ('avro.schema.url'='${schema_output_person_age}');

CREATE TABLE document_with_authors
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output_document_with_authors}'
TBLPROPERTIES ('avro.schema.url'='${schema_output_document_with_authors}');

CREATE TABLE person_with_documents
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output_person_with_documents}'
TBLPROPERTIES ('avro.schema.url'='${schema_output_person_with_documents}');


CREATE TEMPORARY FUNCTION collect_all AS 'eu.dnetlib.iis.common.hive.CollectAll';


insert overwrite table person_age
select age from person;

create table documentWithFlatAuthors
location '${workingDir}/documentWithFlatAuthors'
as
select id, title, authorId
from document lateral view explode(authorIds) ids as authorId;

create table personWithDocumentWithFlatAuthors
location '${workingDir}/peronsWithDocumentWithFlatAuthors'
as
select person.id, person.name, person.age, documentWithFlatAuthors.id as docId, documentWithFlatAuthors.title, documentWithFlatAuthors.authorId from
person left outer join documentWithFlatAuthors
on documentWithFlatAuthors.authorId = person.id;

create table personWithStructDocument
location '${workingDir}/peronsWithStructDocument'
as
select id, if(docId is null, null, named_struct("id", docId, "title", title)) as document
from personWithDocumentWithFlatAuthors;

insert overwrite table person_with_documents
select id, collect_all(document)
from personWithStructDocument
group by id;

create table documentWithStructPerson
location '${workingDir}/documentWithStructPerson'
as
select document.id, document.title, if (personWithDocumentWithFlatAuthors.id is null, null, named_struct("id", personWithDocumentWithFlatAuthors.id, "name", name, "age", age)) as person
from document left outer join personWithDocumentWithFlatAuthors
on document.id = personWithDocumentWithFlatAuthors.docId;

insert overwrite table document_with_authors
select id, title, collect_all(person)
from documentWithStructPerson
group by id, title;
