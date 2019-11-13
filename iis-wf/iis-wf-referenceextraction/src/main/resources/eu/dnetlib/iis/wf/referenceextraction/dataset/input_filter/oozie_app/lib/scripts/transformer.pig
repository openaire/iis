define avro_load_dataset AvroStorage('$schema');

define avro_store_dataset_approved AvroStorage('$schema');
define avro_store_dataset_rejected AvroStorage('$schema');

dataset = load '$input' using avro_load_dataset;

dataset_approved = FILTER dataset BY (resourceTypeValue is not null) and (resourceTypeValue MATCHES '$resource_type_value_to_be_approved_regex');
dataset_rejected = FILTER dataset BY (resourceTypeValue is null) or (NOT(resourceTypeValue MATCHES '$resource_type_value_to_be_approved_regex'));

store dataset_approved into '$output_approved' using avro_store_dataset_approved;
store dataset_rejected into '$output_rejected' using avro_store_dataset_rejected;