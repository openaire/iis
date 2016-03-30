define avro_load_document_content
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_document_content');

define avro_load_document_meta
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_document_meta');

define avro_store_document_content
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_document_content');

documentContent = load '$input_document_content' using avro_load_document_content;
documentMeta = load '$input_document_meta' using avro_load_document_meta;

cachedDocumentId = foreach documentMeta generate id;
cachedDocumentIdDistinct = distinct cachedDocumentId;

joinedDocumentContent = join documentContent by id left, cachedDocumentIdDistinct by id;
joinedFilteredDocumentContent = filter joinedDocumentContent by cachedDocumentIdDistinct::id is null;
documentContentFiltered = foreach joinedFilteredDocumentContent generate documentContent::id as id, documentContent::url as url, documentContent::mimeType as mimeType, documentContent::contentChecksum as contentChecksum, documentContent::contentSizeKB as contentSizeKB;

store documentContentFiltered into '$output_document_content' using avro_store_document_content;
