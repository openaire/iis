Description of the project
--------------------------
This project defines **workflows that join together concrete workflow nodes** defined in other IIS projects.

It defines the following workflows:

- **execution workflow** - the main workflow of IIS which:
	- as an input ingests data coming from Information Space and 
	- as an output produces data that is exported to Information Space.
	- consists of two workflows run on different DNet processing phases: preprocessing and primary
- **training workflow** (in the future) - the secondary, long-running workflow which:
	- as an input ingests training data coming from Information Space and potentially other OpenAIRE+ services (e.g. from a subset of BASE) and
	- as an output produces machine learning "models" to be used in the execution workflow.
