define avro_load_document_to_dataset
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_to_dataset');

define avro_load_document_to_mdstore
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_to_mdstore');

define avro_store_document_to_mdstore
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_to_mdstore');

documentToDataset = load '$input_document_to_dataset' using avro_load_document_to_dataset;
datasetIds = foreach documentToDataset generate datasetId as id;
datasetIdsDistinct = distinct datasetIds;

documentToMDStore = load '$input_document_to_mdstore' using avro_load_document_to_mdstore;

joinedWithMDStore = join datasetIdsDistinct by id, documentToMDStore by documentId;

outputDocumentToMdstore = foreach joinedWithMDStore generate datasetIdsDistinct::id as documentId, documentToMDStore::mdStoreId as mdStoreId;

store outputDocumentToMdstore into '$output_document_to_mdstore' using avro_store_document_to_mdstore;
