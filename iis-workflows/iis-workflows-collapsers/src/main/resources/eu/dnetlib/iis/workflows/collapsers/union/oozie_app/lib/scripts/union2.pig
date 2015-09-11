define avro_load_input
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input');

define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');

input1 = load '$input_1' using avro_load_input;
input2 = load '$input_2' using avro_load_input;

input_union = union input1, input2;

store input_union into '$output' using avro_store_output;
