define avro_load_extracted_metadata AvroStorage('$schema_input');

define avro_store_text AvroStorage('$schema_output');

extr_meta = load '$input' using avro_load_extracted_metadata;

output_text = foreach extr_meta generate 
	id as id, text as text;

store output_text into '$output' using avro_store_text;