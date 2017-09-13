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

unioned = union input_a, input_b;

grouped = GROUP unioned BY ($group_by_field_1, $group_by_field_2);

output_c = FOREACH grouped GENERATE
    FLATTEN(group) AS ($group_by_field_1, $group_by_field_2),
    MAX(unioned.confidenceLevel) AS confidenceLevel;

store output_c into '$output' using avro_store_output;
