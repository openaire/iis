define avro_load_input_citations
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input');

define avro_store_output_citations
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');

define EMPTY_MAP eu.dnetlib.iis.workflows.transformers.udfs.EmptyMap;

input_citations = load '$input' using avro_load_input_citations;

working_citation = foreach input_citations generate 
	sourceDocumentId, 
	position as position:int, 
	null as rawText:chararray, 
	destinationDocumentId, 
	confidenceLevel as confidenceLevel:float, 
	EMPTY_MAP() as emptyMap;

-- notice: we need two steps to create 'entry' tuple, we cannot create it straight away using e.g. 'position as position:int' 
output_citations = foreach working_citation generate 
	sourceDocumentId, (position, rawText, destinationDocumentId, confidenceLevel, emptyMap) as entry;

store output_citations into '$output' using avro_store_output_citations;