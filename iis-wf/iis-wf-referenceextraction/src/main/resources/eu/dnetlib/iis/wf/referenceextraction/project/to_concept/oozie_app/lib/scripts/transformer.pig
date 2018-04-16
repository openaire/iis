define avro_load_document_to_project AvroStorage('$schema_input_document_to_project');

define avro_load_project AvroStorage('$schema_input_project');

define avro_load_concept AvroStorage('$schema_input_concept');

define avro_store_document_to_concept AvroStorage('$schema_output');

documentToProject = load '$input_document_to_project' using avro_load_document_to_project;

project = load '$input_project' using avro_load_project;
concept = load '$input_concept' using avro_load_concept;

conceptFlat = foreach concept generate id as id, flatten(params) as (name, value);
conceptFlatFilteredByProject = filter conceptFlat by (name == 'CD_PROJECT_NUMBER') and (value is not null);
conceptToGrant = foreach conceptFlatFilteredByProject generate id as id, value as grantId;
conceptFlatFilteredByFunder = filter conceptFlat by (name == 'funder') and (value is not null);
conceptToFunder = foreach conceptFlatFilteredByFunder generate id as id, value as funder;
joinedConceptToGrantAndFunder = join conceptToGrant by id, conceptToFunder by id;

conceptToGrantFiltered = foreach joinedConceptToGrantAndFunder generate 
	conceptToGrant::id as id,
	conceptToGrant::grantId as grantId, 
	conceptToFunder::funder as funder;

joinedDoc2ProjWithProject = join documentToProject by projectId left, project by id;
joinedDoc2ProjWithProjectAndConcept = join joinedDoc2ProjWithProject by (project::projectGrantId, UPPER(STRSPLIT(project::fundingClass,'::',2).$0)), conceptToGrantFiltered by (grantId, UPPER(funder));

outputDocumentToConcept = foreach joinedDoc2ProjWithProjectAndConcept generate 
	documentToProject::documentId as documentId,
	conceptToGrantFiltered::id as conceptId, 
	documentToProject::confidenceLevel as confidenceLevel;

outputDocumentToConceptDistinct = distinct outputDocumentToConcept; 

store outputDocumentToConceptDistinct into '$output' using avro_store_document_to_concept;