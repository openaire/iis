define avro_load_input
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema');

input_records = load '$input' using avro_load_input;

output_records = filter input_records by ($threshold_field is not null) AND ($threshold_field >= $threshold_value);

store output_records into '$output' using avro_store_output;
