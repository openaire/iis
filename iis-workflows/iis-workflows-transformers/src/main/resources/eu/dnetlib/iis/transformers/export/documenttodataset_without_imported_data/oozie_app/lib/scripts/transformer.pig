define avro_load_document_to_dataset
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_to_dataset');

define avro_load_document_relation
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_relation');

define avro_store_document_to_dataset
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_to_dataset');


documentToDataset = load '$input_document_to_dataset' using avro_load_document_to_dataset;
documentRelation = load '$input_document_relation' using avro_load_document_relation;

documentToRelation = foreach documentRelation generate id, flatten(referencedIds) as refId;

joined = join documentToDataset by (documentId, datasetId) left, documentToRelation by (id, refId);
subtracted = filter joined by id is null and refId is null;
withoutImported = foreach subtracted generate documentId, datasetId, confidenceLevel;

store withoutImported into '$output_document_to_dataset' using avro_store_document_to_dataset;
