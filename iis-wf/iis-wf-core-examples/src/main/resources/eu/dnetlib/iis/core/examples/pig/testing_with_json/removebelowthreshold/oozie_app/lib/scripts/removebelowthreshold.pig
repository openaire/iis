define avro_load_input_from_project_reference_extraction AvroStorage('$schema_input_from_project_reference_extraction');

define avro_store_output AvroStorage('$schema_output');


input_from_project_reference_extraction = load '$input_from_project_reference_extraction' using avro_load_input_from_project_reference_extraction;

filtered = filter input_from_project_reference_extraction by confidenceLevel > $threshold;

filtered_cleaned = foreach filtered generate
  documentId,
  projectId;

store filtered_cleaned into '$output' using avro_store_output;

