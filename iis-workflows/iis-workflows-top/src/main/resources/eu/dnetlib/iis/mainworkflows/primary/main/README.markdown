Fully functional production workflow including 3 phases:
* importing
* processing
* exporting

Importing phase triggers different kinds of import and preprocessing:
* InformationSpace HBase metadata import
* MDStore dataset metadata import
* ObjectStore PDF, XML PMC, text contents import
* plaintext and metadata extraction performed on imported PDF contents
* plaintext and metadata extraction perfromed on imported XML PMC contents

Processing phase executes mainchain subworkflow encapsulating all KDM definitions. See mainchain README.markdown for details.

Exporting phase consists of two parts:
* inferred data export including relations and entities
* dataset entities export for dataset reference extraction outcome
