define avro_load_metadata AvroStorage('$schema_input');

define avro_store_metadata AvroStorage('$schema_output');

define EMPTY_TO_NULL eu.dnetlib.iis.common.pig.udfs.EmptyBagToNull;

pmc_metadata = load '$input' using avro_load_metadata;

output_metadata = foreach pmc_metadata {
    refsWithFlatMeta = foreach references generate position, flatten(basicMetadata), text;
    parsed_flat_references = foreach refsWithFlatMeta generate 
        position as position:int,
        basicMetadata::authors as refAuthors,
        basicMetadata::title as title:chararray, 
        basicMetadata::source as source:chararray,
        basicMetadata::volume as volume:chararray,
        basicMetadata::year as year:chararray,
        null as edition:chararray,
        null as publisher:chararray,
        null as location:chararray,
        null as series:chararray,
        basicMetadata::issue as issue:chararray,
        null as url:chararray, 
        text as text:chararray,
        basicMetadata::externalIds as externalIds,
        basicMetadata::pages as pages;
    parsed_references = foreach parsed_flat_references generate
      	(title, refAuthors, pages, source, volume, year, edition, publisher, location, series, issue, url, externalIds) as basicMetadata,
      	position as position:int, 
      	text as text:chararray;
    generate id as id,
    	null as title:chararray,
		null as abstract:chararray,
		null as language:chararray,
		EMPTY_TO_NULL(null) as keywords,
		externalIdentifiers as externalIdentifiers,		
       	journal as journal,
		null as year:int,
		null as publisher:chararray,
    	parsed_references as references,
		authors as authors,
		affiliations as affiliations,
		null as volume:chararray,
		null as issue:chararray,
    	pages as pages,
    	entityType as publicationTypeName:chararray,
    	text as text:chararray,
    	null as extractedBy:chararray;
}

store output_metadata into '$output' using avro_store_metadata;