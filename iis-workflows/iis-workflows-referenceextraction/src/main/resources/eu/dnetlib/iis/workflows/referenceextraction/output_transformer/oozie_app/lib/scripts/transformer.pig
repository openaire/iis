define avro_load_document_to_research_initiative
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_to_research_initiative');

define avro_store_document_to_research_initiatives
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_to_research_initiatives');

define DEDUPLICATE_IDS_WITH_CONFIDENCE eu.dnetlib.iis.common.pig.udfs.DeduplicateIdsWithConfidence;

documentToResearchInitiative = load '$input_document_to_research_initiative' using avro_load_document_to_research_initiative;

researchInitiativeGroupped = group documentToResearchInitiative by documentId;
researchInitiative = foreach researchInitiativeGroupped {
    idsWithConfidence = foreach documentToResearchInitiative generate conceptId as id, confidenceLevel;
    dedupIdsWithConfidence = DEDUPLICATE_IDS_WITH_CONFIDENCE(idsWithConfidence);
    generate group as documentId, dedupIdsWithConfidence as concepts;
}

store researchInitiative into '$output_document_to_research_initiatives' using avro_store_document_to_research_initiatives;
