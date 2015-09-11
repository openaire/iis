define avro_load_extracted_document_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_extracted_document_metadata');

define avro_load_document_content_url
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_document_content_url');

define avro_store_extracted_document_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_extracted_document_metadata');

sourceDocumentMeta = load '$input_extracted_document_metadata' using avro_load_extracted_document_metadata;
sourceDocumentContentUrl = load '$input_document_content_url' using avro_load_document_content_url;

filteredDocumentContentUrl = filter sourceDocumentContentUrl by contentChecksum is not null;
joined = join filteredDocumentContentUrl by contentChecksum left, sourceDocumentMeta by id;

outputDocumentMeta = foreach joined generate 
		filteredDocumentContentUrl::id as id,
		sourceDocumentMeta::title as title,
		sourceDocumentMeta::abstract as abstract,
		sourceDocumentMeta::language as language,
		sourceDocumentMeta::keywords as keywords,
		sourceDocumentMeta::externalIdentifiers as externalIdentifiers,
		sourceDocumentMeta::journal as journal,
		sourceDocumentMeta::year as year,
		sourceDocumentMeta::publisher as publisher,
		sourceDocumentMeta::references as references,
		sourceDocumentMeta::authors as authors,
		sourceDocumentMeta::affiliations as affiliations,
		sourceDocumentMeta::volume as volume,
		sourceDocumentMeta::issue as issue,
		sourceDocumentMeta::pages as pages,
		sourceDocumentMeta::publicationTypeName as publicationTypeName;

store outputDocumentMeta into '$output' using avro_store_extracted_document_metadata;
