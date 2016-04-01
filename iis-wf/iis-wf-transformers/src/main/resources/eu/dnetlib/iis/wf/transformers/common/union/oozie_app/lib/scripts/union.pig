define avro_load_input_a AvroStorage('$schema');

define avro_load_input_b AvroStorage('$schema');

define avro_store_output AvroStorage('$schema');

input_a = load '$input_a' using avro_load_input_a;
input_b = load '$input_b' using avro_load_input_b;

output_c = union input_a, input_b;

store output_c into '$output' using avro_store_output;
