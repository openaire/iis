define AVRO_LOAD_DATA AvroStorage('$schema_data');

define AVRO_STORE_DATA AvroStorage('$schema_data', '-doublecolons');

data = load '$input' using AVRO_LOAD_DATA;

grouped = GROUP data BY (id, contentChecksum);

deduped = FOREACH grouped {
      top_rec = LIMIT data 1;
      GENERATE FLATTEN(top_rec);
};

store deduped into '$output' using AVRO_STORE_DATA;