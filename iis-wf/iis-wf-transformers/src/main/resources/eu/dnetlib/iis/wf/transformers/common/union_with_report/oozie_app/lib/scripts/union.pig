define avro_load_input_a
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_load_input_b
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema');

input_a = load '$input_a' using avro_load_input_a;
input_b = load '$input_b' using avro_load_input_b;

output_c = union input_a, input_b;

store output_c into '$output' using avro_store_output;
