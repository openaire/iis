PRAGMA temp_store_directory = '.';

create temp table pubs as select * from 
( setschema 'c1,c2'  select jsonpath(c1,'id','text') from 
stdinput() );

-- select jdict('documentId', docid, 'patentId', id, 'confidenceLevel', 0.8,'textsnippet',context) from (


--create temp table results as select * from (
select jdict('documentId', docid, 'appln_nr', id, 'patentcode',patentcode, 'confidenceLevel', 0.8,'textsnippet',context) from (
select  docid, patents.c1 as id, patents.c2 as patentcode, prev||" "||middle||" "||next as context from 
(setschema 'docid,prev,middle,next' select c1 as docid, textwindow2s(keywords(c2),7,1,3, "(?:\D|\b)[A-Z]?\d{6,}(\b|D)") from (setschema 'c1,c2' select * from pubs)), patents
where regexpr("(\d{6,})", middle) = normal  
and (
    regexprmatches("patent",lower(context)) and (regexprmatches("\b"||c4||"\b",context) or regexprmatches("\b"||c4,middle)) and regexprmatches(c2, middle)
    ) 
and (not regexprmatches("holotype|journal pone|journal pntd|paratype|scientometrics|specimen|dissection|\bnih\b|hepth|barcode|\bstrain|accession|\bbacter\b|patent ductus|patent foramen|arxiv|cern|biol|clin|letters|report",lower(context)))
and (not regexprmatches("[0-9]\."||normal,middle) )

);


--select jdict('documentId', docid, 'appln_nr', id, 'confidenceLevel', 0.8,'textsnippet',context,'author',regexprmatches(authors,context))  from results;

