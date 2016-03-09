IIS module importing data from Information Space HBase.

Contains classes and import workflow definitions used for testing purposes. 

**TODO (2015-09-04)**: the input descriptions below are surely outdated since we don't use Protocol Buffers any more

input:		hbase 
output1:	eu.dnetlib.iis.schemas.protobuf.ProjectFundingProto.ProjectFunding
output2:	eu.dnetlib.iis.schemas.protobuf.PersonProto.Person
output3:	eu.dnetlib.iis.schemas.protobuf.DocumentMetadataProto.DocumentMetadata
output4:	eu.dnetlib.iis.schemas.protobuf.DocumentContentProto.DocumentContent

All the mappings between IIS input model and IS model are described on google docs site:
https://docs.google.com/folder/d/0BxyETgjVNSF-R0RBNDFiaGNtT3M/edit?docId=0AuafT1iWZbXddF9nUXFYLUl3bTR0dnd3anBhM3dqSmc
on "CMS_to_IIS" section.
