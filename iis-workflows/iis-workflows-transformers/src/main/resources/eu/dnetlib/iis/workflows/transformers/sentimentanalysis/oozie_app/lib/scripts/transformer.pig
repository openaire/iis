define avro_load_input_extracted_document_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_extracted_document_metadata');

define avro_load_input_text
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_text');

define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');

documentMeta = load '$input_extracted_document_metadata' using avro_load_input_extracted_document_metadata;
documentText = load '$input_text' using avro_load_input_text;

joinedTextWithMeta = join documentMeta by id, documentText by id;
textWithReferences = foreach joinedTextWithMeta generate
	documentMeta::id as id, 
	documentText::text as text,
	documentMeta::references as references;

filteredTextWithReferences = filter textWithReferences by text IS NOT NULL AND references IS NOT NULL;

store filteredTextWithReferences into '$output' using avro_store_output;
