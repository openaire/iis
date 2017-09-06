define avro_load_input_metadata AvroStorage('$schema_input_metadata');

define avro_store_output_document_metadata AvroStorage('$schema_output_document_metadata');

define NULL_EMPTY eu.dnetlib.iis.common.pig.udfs.EmptyBagToNull;

metadata = load '$input_metadata' using avro_load_input_metadata;

output_meta = foreach metadata generate id, title, abstract, keywords, NULL_EMPTY(importedAuthors) as authors;

store output_meta into '$output_document_metadata' using avro_store_output_document_metadata;

