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

documentMetaFiltered = filter documentMeta by publicationTypeName is null OR publicationTypeName != '\$EMPTY$';
joinedDocumentMeta = join documentMetaFiltered by id, documentContentIdDistinct by id;
documentMetaOutput = foreach joinedDocumentMeta generate 
	documentMetaFiltered::id as id, 
	documentMetaFiltered::title as title,
	documentMetaFiltered::abstract as abstract,
	documentMetaFiltered::language as language,
	documentMetaFiltered::keywords as keywords,
	documentMetaFiltered::externalIdentifiers as externalIdentifiers,
	documentMetaFiltered::journal as journal,
	documentMetaFiltered::year as year,
	documentMetaFiltered::publisher as publisher,
	documentMetaFiltered::references as references,
	documentMetaFiltered::authors as authorsMeta,
	documentMetaFiltered::affiliations as affiliations,
	documentMetaFiltered::volume as volume,
	documentMetaFiltered::issue as issue,
	documentMetaFiltered::pages as pages,
	documentMetaFiltered::publicationTypeName as publicationTypeName,
	documentMetaFiltered::text as text,
	documentMetaFiltered::extractedBy as extractedBy;

store documentContentFiltered into '$output_document_content' using avro_store_document_content;
store documentMetaOutput into '$output_document_meta' using avro_store_document_meta;
