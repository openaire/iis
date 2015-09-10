define avro_load_input
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_main');

define avro_load_id_mapping
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input_id_mapping');


define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');


define IDREPLACE eu.dnetlib.iis.transformers.udfs.IdReplacerUDF;


idMapping = load '$input_id_mapping' using avro_load_id_mapping;
main = load '$input' using avro_load_input;

joined = group main by $id_field_to_replace1 inner, idMapping by originalId;
toRewrite = foreach joined generate flatten(idMapping.newId), flatten(main);
joinedEmpty = filter joined by IsEmpty(idMapping);
empty = foreach joinedEmpty generate null, flatten(main);
emptyAndToRewrite = union toRewrite, empty;
replacedIds = foreach emptyAndToRewrite generate flatten(IDREPLACE('$id_field_to_replace1', *));

store replacedIds into '$output' using avro_store_output;
