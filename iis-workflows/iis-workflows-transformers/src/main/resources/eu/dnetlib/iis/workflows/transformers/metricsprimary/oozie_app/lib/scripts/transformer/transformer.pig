define avro_load_document
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_document');

define avro_load_citation
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_citation');

define avro_load_person
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_person');


define avro_store_document_authors_citations
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_authors_citations');

define avro_store_person_id
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '1',
'schema', '$schema_output_person_id');


define CREATE_ARRAY eu.dnetlib.iis.transformers.udfs.NullToEmptyBag;

document = load '$input_document' using avro_load_document;
citation = load '$input_citation' using avro_load_citation;

documentWithAuthors = foreach document generate id, authorIds;

grouppedCitation = group citation by sourceDocumentId;
documentWithRefs = foreach grouppedCitation {
    refs = foreach citation generate destinationDocumentId;
    generate group as id, refs as referencedIds;
}

joined = join documentWithAuthors by id full, documentWithRefs by id;
joinedWithEmptyArrays = foreach joined generate
    documentWithAuthors::id as documentId,
    CREATE_ARRAY(documentWithAuthors::authorIds) as authorIds,
    CREATE_ARRAY(documentWithRefs::referencedIds) as referencedIds;

store joinedWithEmptyArrays into '$output_document_authors_citations' 
    using avro_store_document_authors_citations;

person = load '$input_person' using avro_load_person;

personId = foreach person generate id;
personIdNotNull = filter personId by id is not null;

store personIdNotNull into '$output_person_id' using avro_store_person_id;
