define avro_load_input_a AvroStorage('$schema');

define avro_load_input_b AvroStorage('$schema');

define avro_load_input_c AvroStorage('$schema');

define avro_load_input_d AvroStorage('$schema');

define avro_store_output AvroStorage('$schema');

input_a = load '$input_a' using avro_load_input_a;
input_b = load '$input_b' using avro_load_input_b;
input_c = load '$input_c' using avro_load_input_c;
input_d = load '$input_d' using avro_load_input_d;

output_final = union input_a, input_b, input_c, input_d; 

store output_final into '$output' using avro_store_output;
