define avro_load_input_citations AvroStorage('$schema_input');

define avro_store_output_citations AvroStorage('$schema_output');

input_citations = load '$input' using avro_load_input_citations;

grouppedCitations = group input_citations by sourceDocumentId;
output_citations = foreach grouppedCitations {
    citations = foreach input_citations generate 
    	entry.position, entry.rawText, entry.destinationDocumentId, entry.confidenceLevel, entry.externalDestinationDocumentIds;
    orderedCitations = order citations by position;
    generate group as documentId, orderedCitations;
}

store output_citations into '$output' using avro_store_output_citations;
