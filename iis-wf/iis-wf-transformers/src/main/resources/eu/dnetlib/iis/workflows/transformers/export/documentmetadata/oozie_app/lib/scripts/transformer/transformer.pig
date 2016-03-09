define avro_load_extracted_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_extracted_metadata');

define avro_store_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_metadata');

extr_meta = load '$input_extracted_metadata' using avro_load_extracted_metadata;

outputMetadata = foreach extr_meta generate 
		id as id,
        affiliations as affiliations;

filteredOutputMetadata = filter outputMetadata by affiliations is not null;

store filteredOutputMetadata into '$output_metadata' using avro_store_metadata;