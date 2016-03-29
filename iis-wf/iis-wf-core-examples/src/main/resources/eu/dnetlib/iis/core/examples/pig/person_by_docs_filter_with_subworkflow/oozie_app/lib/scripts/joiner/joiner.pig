define AVRO_LOAD_PERSON AvroStorage('$schema_input_person');

define AVRO_LOAD_DOCUMENT AvroStorage('$schema_input_document');

define AVRO_STORE_AUTHORS AvroStorage('$schema_output_person_with_documents');

people = load '$input_person' using AVRO_LOAD_PERSON;
docs = load '$input_document' using AVRO_LOAD_DOCUMENT;

flattenDocs = foreach docs generate id as id, title as title, flatten(authorIds) as authorId;
peopleWithDocs = join people by id left outer, flattenDocs by authorId;
peopleWithDocsLessInfo = foreach peopleWithDocs generate people::id as peopleId, flattenDocs::id as docsId, title as title;
grouppedPeopleWithDocs = group peopleWithDocsLessInfo by peopleId;
authorsWithDocs = foreach grouppedPeopleWithDocs {
    docsWithNulls = foreach $1 generate $1, $2;
    cleanedDocs = filter docsWithNulls by $0 is not null;
    generate $0 as personId, cleanedDocs as documents;
};

store authorsWithDocs into '$output_person_with_documents' using AVRO_STORE_AUTHORS;
