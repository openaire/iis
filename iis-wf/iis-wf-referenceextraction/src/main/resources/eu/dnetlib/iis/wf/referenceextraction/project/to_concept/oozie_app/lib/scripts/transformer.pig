define avro_load_document_to_project AvroStorage('$schema_input_document_to_project');

define avro_load_project AvroStorage('$schema_input_project');

define avro_load_concept AvroStorage('$schema_input_concept');

define avro_store_document_to_concept AvroStorage('$schema_output');

documentToProject = load '$input_document_to_project' using avro_load_document_to_project;

project = load '$input_project' using avro_load_project;
concept = load '$input_concept' using avro_load_concept;

conceptFlat = foreach concept generate id as id, flatten(params) as (name, value);
conceptFlatFiltered = filter conceptFlat by (name == '$grant_id_param_name') and (value is not null);
conceptToGrant = foreach conceptFlatFiltered generate id as id, value as grantId;

joinedDoc2ProjWithProject = join documentToProject by projectId left, project by id;
joinedDoc2ProjWithProjectAndConcept = join joinedDoc2ProjWithProject by project::projectGrantId, conceptToGrant by grantId;

outputDocumentToConcept = foreach joinedDoc2ProjWithProjectAndConcept generate 
	documentToProject::documentId as documentId,
	conceptToGrant::id as conceptId, 
	documentToProject::confidenceLevel as confidenceLevel;

outputDocumentToConceptDistinct = distinct outputDocumentToConcept; 

store outputDocumentToConceptDistinct into '$output' using avro_store_document_to_concept;