define AVRO_LOAD_COMMUNITY
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_community');

define AVRO_LOAD_DOCUMENT_ID
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_document_id');

define AVRO_STORE_COMMUNITY
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_community');

communities = load '$input_community' using AVRO_LOAD_COMMUNITY;
ids = load '$input_document_id' using AVRO_LOAD_DOCUMENT_ID as (id:chararray);

existentCommunities = join communities by SimilarDocid, ids by id;
outputCommunities = foreach existentCommunities generate communities::SimilarDocid as SimilarDocid, communities::cid as cid;

store outputCommunities into '$output' using AVRO_STORE_COMMUNITY;