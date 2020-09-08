define avro_load_project AvroStorage('$schema');

define avro_store_project_approved AvroStorage('$schema');
define avro_store_project_rejected AvroStorage('$schema');

project = load '$input' using avro_load_project;

output_approved = FILTER project BY (fundingClass is null) or (NOT(fundingClass MATCHES '$fundingclass_blacklist_regex'));
output_rejected = FILTER project BY (fundingClass is not null) AND (fundingClass MATCHES '$fundingclass_blacklist_regex');

store output_approved into '$output_approved' using avro_store_project_approved;
store output_rejected into '$output_rejected' using avro_store_project_rejected;