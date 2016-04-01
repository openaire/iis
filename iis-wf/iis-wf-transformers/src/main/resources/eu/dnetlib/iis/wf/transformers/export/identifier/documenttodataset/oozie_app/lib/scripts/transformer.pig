define avro_load_document_to_dataset AvroStorage('$schema_input_document_to_dataset');

define avro_load_document_to_mdstore AvroStorage('$schema_input_document_to_mdstore');

define avro_store_document_to_mdstore AvroStorage('$schema_output_document_to_mdstore');

documentToDataset = load '$input_document_to_dataset' using avro_load_document_to_dataset;
datasetIds = foreach documentToDataset generate datasetId as id;
datasetIdsDistinct = distinct datasetIds;

documentToMDStore = load '$input_document_to_mdstore' using avro_load_document_to_mdstore;

joinedWithMDStore = join datasetIdsDistinct by id, documentToMDStore by documentId;

outputDocumentToMdstore = foreach joinedWithMDStore generate datasetIdsDistinct::id as documentId, documentToMDStore::mdStoreId as mdStoreId;

store outputDocumentToMdstore into '$output_document_to_mdstore' using avro_store_document_to_mdstore;
