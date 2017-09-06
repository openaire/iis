define avro_load_base_metadata AvroStorage('$schema_input_base_metadata');

define avro_load_extracted_metadata AvroStorage('$schema_input_extracted_metadata');

define avro_store_merged_metadata AvroStorage('$schema_output_merged_metadata');

define FIRST_NOT_NULL_STR eu.dnetlib.iis.common.pig.udfs.StringFirstNotEmpty;
define FIRST_NOT_NULL_INT eu.dnetlib.iis.common.pig.udfs.IntegerFirstNotEmpty;
define MERGE_ARRAYS eu.dnetlib.iis.common.pig.udfs.StringBagsMerger;
define MERGE_HASHES eu.dnetlib.iis.common.pig.udfs.StringMapsMerger;
define EMPTY_TO_NULL eu.dnetlib.iis.common.pig.udfs.EmptyBagToNull;


base_meta = load '$input_base_metadata' using avro_load_base_metadata;
extr_meta = load '$input_extracted_metadata' using avro_load_extracted_metadata;

joined = join base_meta by id full, extr_meta by id;

merged = foreach joined {
    authorNames = foreach extr_meta::authors generate authorFullName;
    generate
         FIRST_NOT_NULL_STR(base_meta::id, extr_meta::id) as id, 
         FIRST_NOT_NULL_STR(base_meta::title, extr_meta::title) as title,
         FIRST_NOT_NULL_STR(base_meta::abstract, extr_meta::abstract) as abstract,
         FIRST_NOT_NULL_STR(base_meta::language, extr_meta::language) as language,
         MERGE_ARRAYS(base_meta::keywords, extr_meta::keywords) as keywords,
         MERGE_HASHES(base_meta::externalIdentifiers, extr_meta::externalIdentifiers) as externalIdentifiers,
         FIRST_NOT_NULL_STR(base_meta::journal, extr_meta::journal) as journal,
         FIRST_NOT_NULL_INT(base_meta::year, extr_meta::year) as year,
         FIRST_NOT_NULL_STR(base_meta::publisher, extr_meta::publisher) as publisher,
         base_meta::publicationType as publicationType,
         extr_meta::references as references,
         EMPTY_TO_NULL(authorNames) as extractedAuthorFullNames,
         base_meta::authors as importedAuthors,
         extr_meta::volume as volume,
         extr_meta::issue as issue,
         extr_meta::pages as pages,
         extr_meta::publicationTypeName as publicationTypeName;
    }

mergedWithNullType = filter merged by publicationType is null;
mergedWithFalseType = foreach mergedWithNullType generate
         id, title, abstract, language, keywords, externalIdentifiers, journal, year, publisher,
         false as isArticle, false as isDataset, references, extractedAuthorFullNames,
         importedAuthors, volume, issue, pages, publicationTypeName;

mergedWithFalseStructType = foreach mergedWithFalseType generate
         id, title, abstract, language, keywords, externalIdentifiers, journal, year, publisher,
         (isArticle, isDataset) as publicationType, references, extractedAuthorFullNames,
         importedAuthors, volume, issue, pages, publicationTypeName;

mergedWithNotNullType = filter merged by publicationType is not null;

mergedAll = union mergedWithNotNullType, mergedWithFalseStructType;

store mergedAll into '$output_merged_metadata' using avro_store_merged_metadata;