define avro_load_person
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_person');

define avro_load_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_metadata');

define avro_store_citation_metadata
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output_citation_metadata');

define CREATE_ARRAY eu.dnetlib.iis.transformers.udfs.NullToEmptyBag;
define LIST_TO_INDEXED_LIST eu.dnetlib.iis.transformers.udfs.StringListToListWithIndexes;
define FIRST_NOT_EMPTY eu.dnetlib.iis.transformers.udfs.StringFirstNotEmpty;

person = load '$input_person' using avro_load_person;
metadata = load '$input_metadata' using avro_load_metadata;

docWithIndexedAuthors = foreach metadata generate id, flatten(LIST_TO_INDEXED_LIST(authorIds)) as (index, authorId);
docWithAuthorsJoined = join docWithIndexedAuthors by authorId left, person by id;
docWithAuthorsJoinedCleaned = foreach docWithAuthorsJoined generate 
    docWithIndexedAuthors::id as id, 
    docWithIndexedAuthors::index as index, 
    person::fullname as author;
docWithAuthorsGroupped = group docWithAuthorsJoinedCleaned by id;
docWithFullnames = foreach docWithAuthorsGroupped {
    orderedAuthors = order docWithAuthorsJoinedCleaned by index;
    authorsWithUndefined = foreach orderedAuthors generate FIRST_NOT_EMPTY(author, '_UNDEFINED_') as author;
    generate group as id, authorsWithUndefined as authors;
}

docFlatMetadataWithStructuredPages = foreach metadata generate 
    id, (chararray)year, title, journal, references, flatten(pages) as (start, end);
docFlatMetadata = foreach docFlatMetadataWithStructuredPages generate 
    id, title, journal, references, CONCAT(CONCAT(start, '-'), end) as pages, year;

joined = join docFlatMetadata by id left, docWithFullnames by id;
renamedJoined = foreach joined generate 
    docFlatMetadata::id as id,
    CREATE_ARRAY(docWithFullnames::authors) as authors,
    docFlatMetadata::title as title,
    docFlatMetadata::journal as journal,
    docFlatMetadata::pages as pages,
    docFlatMetadata::year as year,
    docFlatMetadata::references as references;
docBasicMetadataAndRefs = foreach renamedJoined generate 
    id, references, (authors, title, journal, pages, year) as basicMetadata;

docCitationMetadata = foreach docBasicMetadataAndRefs {
    refsWithFlatMeta = foreach references generate position, flatten(basicMetadata), text;
    refsWithFlatPages = foreach refsWithFlatMeta generate position, 
        basicMetadata::authors as authors, basicMetadata::title as title, 
        basicMetadata::source as journal, basicMetadata::year as year, text as rawText,
        flatten(basicMetadata::pages) as (start, end);
    flatRefs = foreach refsWithFlatPages generate position, title, journal, year, rawText,
        CREATE_ARRAY(authors) as authors, CONCAT(CONCAT(start, '-'), end) as pages;
    references = foreach flatRefs generate position, 
        (authors, title, journal, pages, year) as basicMetadata, rawText;
    generate id, basicMetadata, references as references;
};

store docCitationMetadata into '$output_citation_metadata' using avro_store_citation_metadata;
