define avro_load_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema');

define avro_store_project
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema');

project = load '$input' using avro_load_project;

outputProject = FILTER project BY (fundingClass is null) or (NOT(fundingClass MATCHES '$fundingclass_blacklist_regex'));

store outputProject into '$output' using avro_store_project;