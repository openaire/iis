define avro_load_dataset AvroStorage('$schema');

define avro_store_dataset_approved AvroStorage('$schema');
define avro_store_dataset_rejected AvroStorage('$schema');

dataset = load '$input' using avro_load_dataset;

X = FOREACH dataset {
        S = FILTER instanceTypes BY $0 == '$instance_type_to_be_approved';
        GENERATE *, COUNT(S.$0) as hits;
}

dataset_approved = FILTER X BY hits > 0;
dataset_rejected = FILTER X BY hits == 0;

store dataset_approved into '$output_approved' using avro_store_dataset_approved;
store dataset_rejected into '$output_rejected' using avro_store_dataset_rejected;