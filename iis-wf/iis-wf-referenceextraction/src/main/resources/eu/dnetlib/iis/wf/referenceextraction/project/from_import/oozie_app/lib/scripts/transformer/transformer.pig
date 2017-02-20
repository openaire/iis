define avro_load_input AvroStorage('$schema_input');
define avro_store_output AvroStorage('$schema_output');

doc_proj_input = load '$input' using avro_load_input;
doc_proj_output = foreach doc_proj_input generate documentId, projectId, (float) 1.0;

store doc_proj_output into '$output' using avro_store_output;