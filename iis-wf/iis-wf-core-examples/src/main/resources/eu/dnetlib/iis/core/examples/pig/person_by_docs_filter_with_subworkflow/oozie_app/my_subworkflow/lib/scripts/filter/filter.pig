define AVRO_LOAD_PERSON AvroStorage('$schema_input_person_with_documents');

define AVRO_STORE_AUTHORS AvroStorage('$schema_output_person_with_documents');

peopleWithDocuments = load '$input_person_with_documents' using AVRO_LOAD_PERSON;
countedPeopleWithDocuments = foreach peopleWithDocuments generate $0 as personId, $1 as documents, COUNT($1) as countDocs;
filteredPeopleWithDocuments = filter countedPeopleWithDocuments by countDocs >= $min_document_number;
filteredPeopleWithDocumentsLessInfo = foreach filteredPeopleWithDocuments generate personId, documents;

store filteredPeopleWithDocumentsLessInfo into '$output_person_with_documents' using AVRO_STORE_AUTHORS;
