input_data = load '$input' using org.apache.pig.piggybank.storage.avro.AvroStorage();
store input_data into '$output' using JsonStorage();
