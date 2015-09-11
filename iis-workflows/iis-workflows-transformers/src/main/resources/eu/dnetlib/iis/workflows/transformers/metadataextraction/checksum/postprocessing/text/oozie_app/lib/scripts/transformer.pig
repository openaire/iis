define avro_load_document_text
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_document_text');

define avro_load_document_content_url
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_document_content_url');

define avro_store_document_text
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_document_text');

sourceDocumentText = load '$input_document_text' using avro_load_document_text;
sourceDocumentContentUrl = load '$input_document_content_url' using avro_load_document_content_url;

filteredDocumentContentUrl = filter sourceDocumentContentUrl by contentChecksum is not null;
joined = join filteredDocumentContentUrl by contentChecksum left, sourceDocumentText by id;

outputDocumentText = foreach joined generate 
		filteredDocumentContentUrl::id as id,
        sourceDocumentText::text as text;

store outputDocumentText into '$output' using avro_store_document_text;
