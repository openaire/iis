define avro_load_merged_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_merged_metadata');


define avro_store_document_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_metadata');


merged_meta = load '$input_merged_metadata' using avro_load_merged_metadata;
abstracts = foreach merged_meta generate id, abstract;
abstracts_without_nulls = filter abstracts by abstract is not null;

store abstracts_without_nulls into '$output_document_metadata' using avro_store_document_metadata;