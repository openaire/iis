define avro_load_document_content AvroStorage('$schema_document_content');

define avro_load_document_text AvroStorage('$schema_document_text');

define avro_store_document_content AvroStorage('$schema_document_content');

define avro_store_document_text AvroStorage('$schema_document_text');

documentContent = load '$input_document_content' using avro_load_document_content;
documentText = load '$input_document_text' using avro_load_document_text;

documentTextId = foreach documentText generate id;

cachedDocumentIdDistinct = distinct documentTextId;

joinedDocumentContent = join documentContent by id left, cachedDocumentIdDistinct by id;
joinedFilteredDocumentContent = filter joinedDocumentContent by cachedDocumentIdDistinct::id is null;
documentContentFiltered = foreach joinedFilteredDocumentContent generate documentContent::id as id, documentContent::url as url, documentContent::mimeType as mimeType, documentContent::contentChecksum as contentChecksum, documentContent::contentSizeKB as contentSizeKB;

documentContentId = foreach documentContent generate id;
documentContentIdDistinct = distinct documentContentId;

joinedDocumentText = join documentText by id, documentContentIdDistinct by id;
documentTextFiltered = foreach joinedDocumentText generate documentText::id as id, documentText::text as text;

store documentContentFiltered into '$output_document_content' using avro_store_document_content;
store documentTextFiltered into '$output_document_text' using avro_store_document_text;
