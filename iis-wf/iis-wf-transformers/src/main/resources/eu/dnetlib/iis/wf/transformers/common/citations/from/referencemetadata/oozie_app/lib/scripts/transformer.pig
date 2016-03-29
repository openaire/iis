define avro_load_input_metadata AvroStorage('$schema_input');

define avro_store_output_citations AvroStorage('$schema_output');

define NullToEmptyBag datafu.pig.bags.NullToEmptyBag();
define EMPTY_MAP eu.dnetlib.iis.common.pig.udfs.EmptyMap;

documentMetadata = load '$input' using avro_load_input_metadata;

docWithRefsFlat = foreach documentMetadata generate id, flatten(NullToEmptyBag(references));
docWithBasicMetadataFlat = foreach docWithRefsFlat generate id, flatten(references::basicMetadata), flatten(references::text), flatten(references::position);

working_citations = foreach docWithBasicMetadataFlat generate
	id as sourceDocumentId,
	references::position as position:int, 
	references::text as rawText:chararray, 
	null as destinationDocumentId:chararray, 
	null as confidenceLevel:float, 
	(references::basicMetadata::externalIds is not null ? references::basicMetadata::externalIds : EMPTY_MAP()) as externalDestinationDocumentIds;
	
output_citations = foreach working_citations generate
	sourceDocumentId, (position, rawText, destinationDocumentId, confidenceLevel, externalDestinationDocumentIds) as entry;
	
store output_citations into '$output' using avro_store_output_citations;