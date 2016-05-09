define avro_load_input_a
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_load_input_b
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_load_input_c
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_load_input_d
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema');

input_a = load '$input_a' using avro_load_input_a;
input_b = load '$input_b' using avro_load_input_b;
input_c = load '$input_c' using avro_load_input_c;
input_d = load '$input_d' using avro_load_input_d;

output_final = union input_a, input_b, input_c, input_d; 

store output_final into '$output' using avro_store_output;