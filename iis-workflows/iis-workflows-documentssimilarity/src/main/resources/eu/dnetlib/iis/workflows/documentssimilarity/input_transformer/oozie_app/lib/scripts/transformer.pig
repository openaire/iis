define avro_load_input_person
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_person');

define avro_load_input_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_metadata');


define avro_store_output_document_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_document_metadata');


define NULL_EMPTY eu.dnetlib.iis.core.pig.udfs.EmptyBagToNull;

person = load '$input_person' using avro_load_input_person;
metadata = load '$input_metadata' using avro_load_input_metadata;

docWithAuthorIdsFlat = foreach metadata generate id, flatten(authorIds) as authorId;

docWithAuthors = join docWithAuthorIdsFlat by authorId, person by id;
docWithAuthorsCleaned = foreach docWithAuthors generate
    docWithAuthorIdsFlat::id as docId,
    person::id as id,
    person::firstname as firstname,
    person::secondnames as secondnames,
    person::fullname as fullname;

docWithAuthorsGrouped = group docWithAuthorsCleaned by docId;
docWithAuthorsArray = foreach docWithAuthorsGrouped {
    authors = foreach docWithAuthorsCleaned generate id, firstname, secondnames, fullname;
    generate group as id, authors;
}

joinedFull = join metadata by id left, docWithAuthorsArray by id;

joinedFullCleaned = foreach joinedFull generate
    metadata::id as id,
    metadata::title as title,
    metadata::abstract as abstract,
    metadata::keywords as keywords,
    docWithAuthorsArray::authors as authors;

store joinedFullCleaned into '$output_document_metadata' using avro_store_output_document_metadata;

