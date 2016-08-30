define avro_load_document_to_dataset AvroStorage('$schema_input_document_to_dataset');

define avro_load_dataset_to_mdstore AvroStorage('$schema_input_dataset_to_mdstore');

define avro_store_dataset_to_mdstore AvroStorage('$schema_output_dataset_to_mdstore');

documentToDataset = load '$input_document_to_dataset' using avro_load_document_to_dataset;
datasetIds = foreach documentToDataset generate datasetId as id;
datasetIdsDistinct = distinct datasetIds;

datasetToMDStore = load '$input_dataset_to_mdstore' using avro_load_dataset_to_mdstore;

joinedWithMDStore = join datasetIdsDistinct by id, datasetToMDStore by datasetId;

outputDatasetToMdstore = foreach joinedWithMDStore generate datasetIdsDistinct::id as datasetId, datasetToMDStore::mdStoreId as mdStoreId;

store outputDatasetToMdstore into '$output_dataset_to_mdstore' using avro_store_dataset_to_mdstore;
