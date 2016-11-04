define avro_load_input AvroStorage('$schema_input');

define avro_store_output AvroStorage('$schema_output');


inputRecords = load '$input' using avro_load_input;

inputWithOrigin = foreach inputRecords generate TOTUPLE(*) as data, '$origin_value' as origin;

store inputWithOrigin into '$output' using avro_store_output;
