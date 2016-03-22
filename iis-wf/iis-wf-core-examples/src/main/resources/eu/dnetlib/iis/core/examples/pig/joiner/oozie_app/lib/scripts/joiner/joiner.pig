define AVRO_LOAD_PERSON
AvroStorage(
'$schema_input_person');

define AVRO_LOAD_DOCUMENT
AvroStorage(
'$schema_input_document');

define AVRO_STORE_DOCS
AvroStorage(
'$schema_output_document_with_authors');

define AVRO_STORE_AUTHORS
AvroStorage(
'$schema_output_person_with_documents');

define AVRO_STORE_AGE
AvroStorage(
'$schema_output_person_age');

people = load '$input_person' using AVRO_LOAD_PERSON;
docs = load '$input_document' using AVRO_LOAD_DOCUMENT;

flattenDocs = foreach docs generate id as id, title as title, flatten(authorIds) as authorId;
docsNulls = foreach docs generate id, title, null as authorId;
flattenDocsWithNulls = union flattenDocs, docsNulls;
docsWithAuthors = join flattenDocsWithNulls by authorId left outer, people by id;
grouppedDocsWithAuthors = group docsWithAuthors by (flattenDocsWithNulls::id, title);
flatDocsWithAuthors = foreach grouppedDocsWithAuthors {
    authorsWithNulls = foreach $1 generate $3 as id, $4 as name, $5 as age;
    authors = filter authorsWithNulls by id is not null;
    generate flatten($0), authors;
};
namedDocsWithAuthors = foreach flatDocsWithAuthors generate $0 as id, $1 as title, $2 as authors;

store namedDocsWithAuthors into '$output_document_with_authors' using AVRO_STORE_DOCS;

peopleWithDocs = join people by id left outer, flattenDocs by authorId;
peopleWithDocsLessInfo = foreach peopleWithDocs generate people::id as peopleId, flattenDocs::id as docsId, title as title;
grouppedPeopleWithDocs = group peopleWithDocsLessInfo by peopleId;
authorsWithDocs = foreach grouppedPeopleWithDocs {
    docsWithNulls = foreach $1 generate $1, $2;
    cleanedDocs = filter docsWithNulls by $0 is not null;
    generate $0, cleanedDocs;
};

store authorsWithDocs into '$output_person_with_documents' using AVRO_STORE_AUTHORS;

personAge = foreach people generate age as age;
dump personAge;

store personAge into '$output_person_age' using AVRO_STORE_AGE;
