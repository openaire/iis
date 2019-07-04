PRAGMA temp_store_directory = '.';
create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();

select jdict('documentId', docid, 'patentId', id, 'confidenceLevel', 0.8,'textsnippet',context) from (

select  docid, appln_id as id, prev||" "||middle||" "||next as context from 
(setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(keywords(c2),10,1,4, "(?:\D|\b)\d{6,12}\b") from (setschema 'c1,c2' select * from pubs)), patents
where regexpr("(?:\D|\b)(\d{6,12})\b",middle) = appln_nr 
and (regexprmatches("european patent|patent application|patent office|ep patent|eu patent|patent",lower(context)) or 
    regexprmatches("\bEPO(?:\d|\b)",context) or regexprmatches ("EP\s*"||appln_nr,context)
    )
and (not regexprmatches("patent ductus|patent foramen|arxiv|cern|biol|clin|letters|report",lower(context)))


);
