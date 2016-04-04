Description of the project
--------------------------
This project defines **workflows that fasten together concrete workflow nodes** defined in other IIS workflow projects.

It defines the following workflows:

- **execution workflow**: `primary` - the main workflow of IIS which:
	- as an input ingest data coming from Information Space and 
	- as an output produce data that is exported to Information Space.
- **training workflow** (not implemented yet) - the secondary, long-running workflow which:
	- as an input ingests training data coming from Information Space and potentially other OpenAIRE services (e.g. from a subset of BASE) and
	- as an output produces machine learning "models" to be used in the execution workflow.
