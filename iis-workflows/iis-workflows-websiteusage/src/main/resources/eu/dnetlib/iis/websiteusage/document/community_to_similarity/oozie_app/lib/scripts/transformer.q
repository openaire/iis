CREATE EXTERNAL TABLE document_to_community
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${input}'
TBLPROPERTIES ('avro.schema.literal'='${schema_input}');

CREATE TABLE similarity
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${output}'
TBLPROPERTIES ('avro.schema.literal'='${schema_output}');

insert overwrite table similarity
select 
	distinct d2c1.SimilarDocid as documentId, d2c2.SimilarDocid as otherDocumentId, null as covisitedSimilarity 
	from document_to_community d2c1 
	join document_to_community d2c2 on (d2c1.cid = d2c2.cid) 
	where d2c1.SimilarDocid!=d2c2.SimilarDocid;
