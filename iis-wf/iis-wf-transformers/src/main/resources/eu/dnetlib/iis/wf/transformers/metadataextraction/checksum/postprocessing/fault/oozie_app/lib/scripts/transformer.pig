define avro_load_fault AvroStorage('$schema_fault');

define avro_load_document_content_url AvroStorage('$schema_document_content_url');

define avro_store_fault AvroStorage('$schema_fault');

sourceFault = load '$input_fault' using avro_load_fault;
sourceDocumentContentUrl = load '$input_document_content_url' using avro_load_document_content_url;

filteredDocumentContentUrl = filter sourceDocumentContentUrl by contentChecksum is not null;
joined = join filteredDocumentContentUrl by contentChecksum, sourceFault by inputObjectId;

outputFault = foreach joined generate 
		filteredDocumentContentUrl::id as inputObjectId,
		sourceFault::timestamp as timestamp,
		sourceFault::code as code,
		sourceFault::message as message,
		sourceFault::stackTrace as stackTrace,
		sourceFault::causes as causes,
		sourceFault::supplementaryData as supplementaryData;

store outputFault into '$output' using avro_store_fault;
