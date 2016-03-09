SET mapred.child.java.opts $mapred_child_java_opts

define avro_load_input
org.apache.pig.piggybank.storage.avro.AvroStorage(
'schema', '$schema_input');

define avro_store_output
org.apache.pig.piggybank.storage.avro.AvroStorage(
'index', '0',
'schema', '$schema_output');


inputRecords = load '$input' using avro_load_input;

inputWithOrigin = foreach inputRecords generate TOTUPLE(*) as data, '$origin_value' as origin;

store inputWithOrigin into '$output' using avro_store_output;
