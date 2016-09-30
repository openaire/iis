define avro_load_document_to_dataset AvroStorage('$schema');

define avro_store_document_to_dataset AvroStorage('$schema', '-doublecolons');

documentToDataset = load '$input' using avro_load_document_to_dataset;

groupedDocumentToDataset = group documentToDataset by (documentId, datasetId);

dedupDocumentToDataset = FOREACH groupedDocumentToDataset {
    srtd = order documentToDataset by confidenceLevel desc;
    srtd_top = LIMIT srtd 1;
    GENERATE FLATTEN(srtd_top);
};

store dedupDocumentToDataset into '$output' using avro_store_document_to_dataset;