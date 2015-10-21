define avro_load_input_extracted_document_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_extracted_document_metadata');

define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');

documentMetaWithText = load '$input_extracted_document_metadata' using avro_load_input_extracted_document_metadata;

textWithReferences = foreach documentMetaWithText generate
	id as id, 
	text as text,
	references as references;

filteredTextWithReferences = filter textWithReferences by references IS NOT NULL;

store filteredTextWithReferences into '$output' using avro_store_output;
