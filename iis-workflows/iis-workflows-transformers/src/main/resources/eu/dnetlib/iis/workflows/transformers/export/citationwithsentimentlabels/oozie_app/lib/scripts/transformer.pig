define avro_load_input_citations
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_citation');

define avro_load_input_sentiment_labels
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_sentiment_labels');

define avro_store_output_citations
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');

define NullToEmptyBag datafu.pig.bags.NullToEmptyBag();

input_citations = load '$input_citation' using avro_load_input_citations;
input_sentiment_labels = load '$input_sentiment_labels' using avro_load_input_sentiment_labels;

flat_citations = foreach input_citations generate sourceDocumentId, flatten(entry);
citations_joined_with_sentiment_labels = join flat_citations by (sourceDocumentId, entry::position) left, input_sentiment_labels by (id, referencePosition);

output_citations = foreach citations_joined_with_sentiment_labels generate
	flat_citations::sourceDocumentId, 
	(flat_citations::entry::position,
	flat_citations::entry::rawText,
	flat_citations::entry::destinationDocumentId, 
	flat_citations::entry::confidenceLevel,
	flat_citations::entry::externalDestinationDocumentIds,
	NullToEmptyBag(input_sentiment_labels::labels)) as entry;

store output_citations into '$output' using avro_store_output_citations;
