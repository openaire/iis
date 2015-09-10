define avro_load_document_to_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_to_project');

define avro_load_imported_document_to_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_imported_document_to_project');


define avro_store_document_to_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_to_project');


documentToProject = load '$input_document_to_project' using avro_load_document_to_project;
importedDocumentToProject = load '$input_imported_document_to_project' using avro_load_imported_document_to_project;

joined = join documentToProject by (documentId, projectId) left, importedDocumentToProject by (documentId, projectId);
filtered = filter joined by importedDocumentToProject::documentId is null and importedDocumentToProject::projectId is null;
withoutImported = foreach filtered generate documentToProject::documentId, documentToProject::projectId, confidenceLevel;

store withoutImported into '$output_document_to_project' using avro_store_document_to_project;
