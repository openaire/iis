input_from_project_reference_extraction = load '../data/input_from_project_reference_extraction.json'
  using JsonLoader('documentId:chararray,projectId:chararray,confidenceLevel:double');

filtered = filter input_from_project_reference_extraction by confidenceLevel > $threshold;

filtered_cleaned = foreach filtered generate
  documentId,
  projectId;

store filtered_cleaned into 'actualOutput' using JsonStorage();
