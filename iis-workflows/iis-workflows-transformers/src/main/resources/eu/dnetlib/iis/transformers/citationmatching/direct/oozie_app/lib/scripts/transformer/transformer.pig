define avro_load_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input');

define avro_store_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');

metadata = load '$input' using avro_load_metadata;

output_metadata = foreach metadata {
    refsWithFlatMeta = foreach references generate position, flatten(basicMetadata);
    parsed_flat_references = foreach refsWithFlatMeta generate 
        position as position:int,
        basicMetadata::externalIds as externalIds;
	parsed_references = filter parsed_flat_references by 
		(externalIds IS NOT NULL) AND (NOT IsEmpty(externalIds));
    generate id as id,
		externalIdentifiers as externalIdentifiers,
		publicationTypeName as publicationTypeName,
    	parsed_references as references;
}

store output_metadata into '$output' using avro_store_metadata;