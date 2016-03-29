define avro_load_document_content_url AvroStorage('$schema_document_content_url');

define avro_store_document_content_url AvroStorage('$schema_document_content_url');

sourceDocumentContentUrl = load '$input' using avro_load_document_content_url;

filteredDocumentContentUrl = filter sourceDocumentContentUrl by contentChecksum is not null;

groupedFilteredDocumentContentUrl = GROUP filteredDocumentContentUrl by contentChecksum;

-- this piece of script is responsible for removing all checksum duplicates
dedupGroupedFilteredDocumentContentUrl = FOREACH groupedFilteredDocumentContentUrl {
      top_record = LIMIT filteredDocumentContentUrl 1;
      GENERATE FLATTEN(top_record);
};

identifiedByChecksumDocumentContentUrl = foreach dedupGroupedFilteredDocumentContentUrl generate 
		contentChecksum as id,
		url as url,
		mimeType as mimeType,
        contentChecksum as contentChecksum,
        contentSizeKB as contentSizeKB;

store identifiedByChecksumDocumentContentUrl into '$output' using avro_store_document_content_url;
