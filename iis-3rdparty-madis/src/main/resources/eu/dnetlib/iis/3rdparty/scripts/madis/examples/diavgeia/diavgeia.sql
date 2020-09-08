-- This is a simple example of harvesting Greek government decisions data from http://opendata.diavgeia.gov.gr/

-- First of all put file diavgeiaget.py in functionslocal/vtable/

-- Following query fetches the data

output 'rawdata.txt' select * from (diavgeiaget);

-- If the transmission breaks (it happens very often) then continue by using
-- output 'rawdataY.txt' select * from (diavgeiaget datefrom:DD-MM-YYYY);
-- where Y is an increasing number (so as to not overwrite previously fetched data), and you can get DD-MM-YYYY from the last <ns3:submissionTimestamp>YYYY-MM-DD....</ns3:submissionTimestamp> field in the data you've recieved

-- After fetching all of the above 'rawdataY' files, concatenate them into a single one ('rawdata.txt').


-- We then want to find out which XML keys the 'rawdata' file contains. We restrict our key search to keys under "decisionexpanded".

select jgroupunion(jdictkeys(c1)) from (xmlparse root:decisionexpanded select utf8clean(c1) from (file 'rawdata.txt'));

-- ["decisions/decisionexpanded/ada","decisions/decisionexpanded/submissiontimestamp","decisions/decisionexpanded/metadata/protocolnumber","decisions/decisionexpanded/metadata/date","decisions/decisionexpanded/metadata/subject","decisions/decisionexpanded/metadata/organization/@/uid","decisions/decisionexpanded/metadata/organization/label","decisions/decisionexpanded/metadata/organization/latinname","decisions/decisionexpanded/metadata/organizationunit/@/uid","decisions/decisionexpanded/metadata/organizationunit/label","decisions/decisionexpanded/metadata/decisiontype/@/uid","decisions/decisionexpanded/metadata/decisiontype/label","decisions/decisionexpanded/metadata/tags/tag/@/uid","decisions/decisionexpanded/metadata/tags/tag/label","decisions/decisionexpanded/metadata/relativefek/issue","decisions/decisionexpanded/metadata/relativefek/year","decisions/decisionexpanded/url","decisions/decisionexpanded/documenturl","decisions/decisionexpanded/metadata/relativefek/feknumber","decisions/decisionexpanded/metadata/extrafields/extrafield/@/name","decisions/decisionexpanded/metadata/extrafields/extrafield/label","decisions/decisionexpanded/metadata/extrafields/extrafield/value","decisions/decisionexpanded/metadata/tags/tag/@/nil","decisions/decisionexpanded/metadata/iscorrectedbyada","decisions/decisionexpanded/metadata/relativeada","decisions/decisionexpanded/metadata/organization/units/unit/@/uid","decisions/decisionexpanded/metadata/organization/units/unit/label","decisions/decisionexpanded/metadata/iscorrectionofada"]


-- We then create the "decisions" table using the keys from above that interest us

create table decisions as 
select * from (
xmlparse '["decisionexpanded/ada","decisionexpanded/submissiontimestamp","decisionexpanded/metadata/protocolnumber","decisionexpanded/metadata/date","decisionexpanded/metadata/subject","decisionexpanded/metadata/organization/@/uid","decisionexpanded/metadata/organization/label","decisionexpanded/metadata/organization/latinname","decisionexpanded/metadata/organizationunit/@/uid","decisionexpanded/metadata/organizationunit/label","decisionexpanded/metadata/decisiontype/@/uid","decisionexpanded/metadata/decisiontype/label","decisionexpanded/metadata/tags/tag/@/uid","decisionexpanded/metadata/tags/tag/label","decisionexpanded/metadata/relativefek/issue","decisionexpanded/metadata/relativefek/year","decisionexpanded/url","decisionexpanded/documenturl","decisionexpanded/metadata/relativefek/feknumber","decisionexpanded/metadata/extrafields/extrafield/@/name","decisionexpanded/metadata/extrafields/extrafield/label","decisionexpanded/metadata/extrafields/extrafield/value","decisionexpanded/metadata/tags/tag/@/nil","decisionexpanded/metadata/iscorrectedbyada","decisionexpanded/metadata/relativeada","decisionexpanded/metadata/organization/units/unit/@/uid","decisionexpanded/metadata/organization/units/unit/label","decisionexpanded/metadata/iscorrectionofada"]'
select utf8clean(c1) from
(file 'rawdata.txt')
);


-- Finally we delete the duplicate ada (decision ids)

delete from decisions where rowid not in (
select rowid from decisions group by ada
);




