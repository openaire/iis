define AVRO_LOAD_DATA AvroStorage('$schema_data');

define AVRO_LOAD_EXISTENT_IDS AvroStorage('$schema_input_existent_id');

define AVRO_STORE_DATA AvroStorage('$schema_data', '-doublecolons');

data = load '$input_data' using AVRO_LOAD_DATA;
ids = load '$input_existent_id' using AVRO_LOAD_EXISTENT_IDS;

existentData = join data by id, ids by id;
outputData = foreach existentData generate data::id as id, url, mimeType, contentChecksum, contentSizeKB;

store outputData into '$output_filtered' using AVRO_STORE_DATA;