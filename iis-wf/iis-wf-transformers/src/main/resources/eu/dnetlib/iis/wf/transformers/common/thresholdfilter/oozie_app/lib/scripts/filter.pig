define avro_load_input AvroStorage('$schema');

define avro_store_output AvroStorage('$schema');

input_records = load '$input' using avro_load_input;

output_records = filter input_records by ($threshold_field is not null) AND ($threshold_field >= $threshold_value);

store output_records into '$output' using avro_store_output;
