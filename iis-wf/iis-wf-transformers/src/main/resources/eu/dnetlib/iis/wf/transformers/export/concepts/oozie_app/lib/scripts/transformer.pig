define avro_load_concept AvroStorage('$schema_input');
define avro_store_concepts AvroStorage('$schema_output');

define DEDUPLICATE_IDS_WITH_CONFIDENCE eu.dnetlib.iis.common.pig.udfs.IdConfidenceTupleDeduplicator;

concept = load '$input' using avro_load_concept;

conceptGroupped = group concept by documentId;
concepts = foreach conceptGroupped {
    idsWithConfidence = foreach concept generate conceptId as id, confidenceLevel;
    dedupIdsWithConfidence = DEDUPLICATE_IDS_WITH_CONFIDENCE(idsWithConfidence);
    generate group as documentId, dedupIdsWithConfidence as concepts;
}

store concepts into '$output' using avro_store_concepts;
