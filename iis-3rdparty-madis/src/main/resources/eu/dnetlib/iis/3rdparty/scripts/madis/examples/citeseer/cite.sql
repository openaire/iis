-- This is a simple example of bulk harvesting an OAI-PMH service. The data are then converted from XML to a single relational table.
-- Due to the extreme slowness and unreliability of OAI-PMH protocol, we first fetch all data into a plain text file. If the transmission breaks, you should continue the transfer on a different file (e.g. 'rawdata1.txt'), so as to no overwrite previously fetched data.

output 'rawdata.txt' select * from (oaiget metadataPrefix:oai_dc 'http://citeseerx.ist.psu.edu/oai2');

-- After fetching all of the above 'rawdataX' files, concatenate them into a single one ('rawdata.txt').


-- We then want to find out which XML keys the 'rawdata' file contains. We restrict our key search to keys under "record".

select jgroupunion(jdictkeys(c1)) from (xmlparse root:record fast:1 file 'rawdata.txt.gz');

-- ["record/header/identifier","record/header/datestamp","record/metadata/dc/@/schemalocation","record/metadata/dc/title","record/metadata/dc/creator","record/metadata/dc/subject","record/metadata/dc/description","record/metadata/dc/contributor","record/metadata/dc/publisher","record/metadata/dc/date","record/metadata/dc/format","record/metadata/dc/type","record/metadata/dc/identifier","record/metadata/dc/source","record/metadata/dc/language","record/metadata/dc/relation","record/metadata/dc/rights"]


-- Finally we create the "article" table using the keys from above that interest us

create table article as 
select regexpr(':([^:]*)$', header_identifier) as header_identifier, header_datestamp, metadata_dc_schemalocation, metadata_dc_title, metadata_dc_creator, metadata_dc_subject, metadata_dc_description, metadata_dc_contributor, metadata_dc_publisher, metadata_dc_date, metadata_dc_format, metadata_dc_type, metadata_dc_identifier, metadata_dc_source, metadata_dc_language, metadata_dc_relation, metadata_dc_rights 
from (
xmlparse strict:0 '["record/header/identifier","record/header/datestamp","record/metadata/dc/@/schemalocation","record/metadata/dc/title","record/metadata/dc/creator","record/metadata/dc/subject","record/metadata/dc/description","record/metadata/dc/contributor","record/metadata/dc/publisher","record/metadata/dc/date","record/metadata/dc/format","record/metadata/dc/type","record/metadata/dc/identifier","record/metadata/dc/source","record/metadata/dc/language","record/metadata/dc/relation","record/metadata/dc/rights"]' 
file 'rawdata.txt' );
