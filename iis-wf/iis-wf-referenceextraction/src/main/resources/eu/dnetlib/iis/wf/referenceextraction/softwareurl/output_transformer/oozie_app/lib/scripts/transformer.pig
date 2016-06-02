define avro_load AvroStorage('$schema_input');

define avro_store AvroStorage('$schema_output');

define DEDUPLICATE_IDS_WITH_CONFIDENCE eu.dnetlib.iis.common.pig.udfs.DeduplicateIdsWithConfidence;

documentToSoftwareUrl = load '$input' using avro_load;

documentToSoftwareUrlGroupped = group documentToSoftwareUrl by documentId;
softwareUrls = foreach documentToSoftwareUrlGroupped {
    urlsWithConfidence = foreach documentToSoftwareUrl generate softwareUrl, repositoryName, confidenceLevel;
    dedupUrlsWithConfidence = DEDUPLICATE_IDS_WITH_CONFIDENCE(urlsWithConfidence);
    generate group as documentId, dedupUrlsWithConfidence as softwareUrls;
}

store softwareUrls into '$output' using avro_store;
