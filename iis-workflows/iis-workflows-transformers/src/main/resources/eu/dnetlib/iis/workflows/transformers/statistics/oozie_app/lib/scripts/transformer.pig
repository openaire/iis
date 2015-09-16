define avro_load_document
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document');

define avro_load_citation
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_citation');

define avro_load_person
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_person');

define avro_load_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_project');

define avro_load_document_to_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document_to_project');


define avro_store_document_authors_citations
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_authors_citations');

define avro_store_person_id
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '1',
'schema', '$schema_output_person_id');

define avro_store_project_id
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '2',
'schema', '$schema_output_project_id');


define CREATE_ARRAY eu.dnetlib.iis.workflows.transformers.udfs.NullToEmptyBag;

document = load '$input_document' using avro_load_document;
citation = load '$input_citation' using avro_load_citation;
documentToProject = load '$input_document_to_project' using avro_load_document_to_project;

documentWithAuthorsAndType = foreach document generate 
    id, year, authorIds, flatten(publicationType);

grouppedCitation = group citation by sourceDocumentId;
documentWithRefs = foreach grouppedCitation {
    refs = foreach citation generate destinationDocumentId;
    refsFiltered = filter refs by destinationDocumentId is not null;
    generate group as id, refsFiltered as referencedIds;
}

joinedRefs = join documentWithAuthorsAndType by id left outer, documentWithRefs by id;
joinedRefsWithEmptyArrays = foreach joinedRefs generate
    documentWithAuthorsAndType::id as documentId,
    documentWithAuthorsAndType::publicationType::article as isPublished,
    (documentWithAuthorsAndType::year is null ? 'unknown' : (chararray)documentWithAuthorsAndType::year) as year,
    CREATE_ARRAY(documentWithAuthorsAndType::authorIds) as authorIds,
    CREATE_ARRAY(documentWithRefs::referencedIds) as referencedDocumentsIds;

grouppedProject = group documentToProject by documentId;
documentWithProjects = foreach grouppedProject {
    projs = foreach documentToProject generate projectId;
    generate group as id, projs as projectIds;
}

joinedProjs = join joinedRefsWithEmptyArrays by documentId left outer, documentWithProjects by id;
joinedProjsWithEmptyArrays = foreach joinedProjs generate
    joinedRefsWithEmptyArrays::documentId as documentId,
    joinedRefsWithEmptyArrays::isPublished as isPublished,
    joinedRefsWithEmptyArrays::year as year,
    joinedRefsWithEmptyArrays::authorIds as authorIds,
    joinedRefsWithEmptyArrays::referencedDocumentsIds as referencedDocumentsIds,
    CREATE_ARRAY(documentWithProjects::projectIds) as projectIds;

store joinedProjsWithEmptyArrays into '$output_document_authors_citations' 
    using avro_store_document_authors_citations;

person = load '$input_person' using avro_load_person;
personId = foreach person generate id;
store personId into '$output_person_id' using avro_store_person_id;

project = load '$input_project' using avro_load_project;
projectId = foreach project generate id;
store projectId into '$output_project_id' using avro_store_project_id;