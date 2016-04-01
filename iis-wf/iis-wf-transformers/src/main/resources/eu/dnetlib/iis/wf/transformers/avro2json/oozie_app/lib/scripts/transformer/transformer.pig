input_data = load '$input' using AvroStorage();
store input_data into '$output' using JsonStorage();
