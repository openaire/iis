define avro_load_project AvroStorage('$schema');

define avro_store_project AvroStorage('$schema');

project = load '$input' using avro_load_project;

outputProject = FILTER project BY (fundingClass is null) or (NOT(fundingClass MATCHES '$fundingclass_blacklist_regex'));

store outputProject into '$output' using avro_store_project;