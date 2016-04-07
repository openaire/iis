define avro_load_document_content AvroStorage('$schema_document_content');

define avro_load_document_meta AvroStorage('$schema_document_meta');

define avro_store_document_content AvroStorage('$schema_document_content');

define avro_store_document_meta AvroStorage('$schema_document_meta');

documentContent = load '$input_document_content' using avro_load_document_content;
documentMeta = load '$input_document_meta' using avro_load_document_meta;

cachedDocumentId = foreach documentMeta generate id;
cachedDocumentIdDistinct = distinct cachedDocumentId;

joinedDocumentContent = join documentContent by id left, cachedDocumentIdDistinct by id;
joinedFilteredDocumentContent = filter joinedDocumentContent by cachedDocumentIdDistinct::id is null;
documentContentFiltered = foreach joinedFilteredDocumentContent generate documentContent::id as id, documentContent::url as url, documentContent::mimeType as mimeType, documentContent::contentChecksum as contentChecksum, documentContent::contentSizeKB as contentSizeKB;

documentContentId = foreach documentContent generate id;
documentContentIdDistinct = distinct documentContentId;

joinedDocumentMeta = join documentMeta by id, documentContentIdDistinct by id;
documentMetaFiltered = foreach joinedDocumentMeta generate 
	documentMeta::id as id, 
	documentMeta::title as title,
	documentMeta::abstract as abstract,
	documentMeta::language as language,
	documentMeta::keywords as keywords,
	documentMeta::externalIdentifiers as externalIdentifiers,
	documentMeta::journal as journal,
	documentMeta::year as year,
	documentMeta::publisher as publisher,
	documentMeta::references as references,
	documentMeta::authors as authorsMeta,
	documentMeta::affiliations as affiliations,
	documentMeta::volume as volume,
	documentMeta::issue as issue,
	documentMeta::pages as pages,
	documentMeta::publicationTypeName as publicationTypeName,
	documentMeta::text as text;

store documentContentFiltered into '$output_document_content' using avro_store_document_content;
store documentMetaFiltered into '$output_document_meta' using avro_store_document_meta;
