define avro_load_document_to_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_to_project');

define avro_load_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_project');

define avro_load_concept
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_concept');

define avro_store_document_to_concept
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');

documentToProject = load '$input_document_to_project' using avro_load_document_to_project;

project = load '$input_project' using avro_load_project;
concept = load '$input_concept' using avro_load_concept;

conceptToGrant = foreach concept generate id as id, params#'$grant_id_param_name' as grantId;
conceptToGrantFiltered = filter conceptToGrant by grantId is not null;

joinedDoc2ProjWithProject = join documentToProject by projectId left, project by id;
joinedDoc2ProjWithProjectAndConcept = join joinedDoc2ProjWithProject by project::projectGrantId, conceptToGrantFiltered by grantId;

outputDocumentToConcept = foreach joinedDoc2ProjWithProjectAndConcept generate 
	documentToProject::documentId as documentId,
	conceptToGrantFiltered::id as conceptId, 
	documentToProject::confidenceLevel as confidenceLevel;

outputDocumentToConceptDistinct = distinct outputDocumentToConcept; 

store outputDocumentToConceptDistinct into '$output' using avro_store_document_to_concept;