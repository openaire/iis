define avro_load_document_to_project AvroStorage('$schema_input_document_to_project');

define avro_store_identifier AvroStorage('$schema_output_identifier');

documentToProject = load '$input_document_to_project' using avro_load_document_to_project;
documentToProjectId = foreach documentToProject generate documentId;
documentToProjectIdDistinct = distinct documentToProjectId;

identifiers = foreach documentToProjectIdDistinct generate documentId as id;

store identifiers into '$output_identifier' using avro_store_identifier;
