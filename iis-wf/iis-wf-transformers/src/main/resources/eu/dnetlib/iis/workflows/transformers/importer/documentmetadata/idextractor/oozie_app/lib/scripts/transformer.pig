define avro_load_document_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_metadata');

define avro_store_identifier
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_identifier');

documentMetadata = load '$input_document_metadata' using avro_load_document_metadata;
documentIds = foreach documentMetadata generate 
        id as id,
        null as dummy:chararray;

store documentIds into '$output_identifier' using avro_store_identifier;
