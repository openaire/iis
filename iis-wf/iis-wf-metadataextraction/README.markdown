IIS module responsible for metadata and fulltext extraction. Based on ICM [CERMINE library](https://github.com/CeON/CERMINE).

Contains all required classes and testing workflow.

**TODO (2015-09-04)**: the input descriptions below are surely outdated since we don't use Protocol Buffers any more

input1:		eu.dnetlib.iis.schemas.protobuf.DocumentContentProto.DocumentContent 
output1:	eu.dnetlib.iis.schemas.protobuf.DocumentWithBasicMetadataProto.DocumentWithBasicMetadata
output2:	eu.dnetlib.iis.schemas.protobuf.DocumentTextProto.DocumentText [disabled by default]

All the mappings between IIS metadata model and Cermine NLM are described on google docs site:
https://docs.google.com/folder/d/0BxyETgjVNSF-R0RBNDFiaGNtT3M/edit?docId=0AuafT1iWZbXddF9nUXFYLUl3bTR0dnd3anBhM3dqSmc
on "cermine_to_iis" section.
