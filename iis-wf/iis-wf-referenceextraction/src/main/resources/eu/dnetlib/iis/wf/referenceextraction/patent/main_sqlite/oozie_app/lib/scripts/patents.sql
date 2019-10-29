PRAGMA temp_store_directory = '.';

create temp table pubs as select c1,c2,jmergeregexp(jset(s2j(comprspaces(regexpr("\b\w{1,3}\b",keywords(c3),""))))) as c3 from 
( setschema 'c1,c2,c3'  select jsonpath(c1,'id','text','authors') from 
stdinput() );



-- select jdict('documentId', docid, 'patentId', id, 'confidenceLevel', 0.8,'textsnippet',context) from (


create temp table results as select * from (
select  docid, authors, appln_id as id, prev||" "||middle||" "||next as context, appln_nr from 
(setschema 'docid,prev,middle,next' select c1 as docid,c3 as authors, textwindow2s(keywords(c2),15,1,7, "(?:\D|\b)\d{6,12}\b") from (setschema 'c1,c2,c3' select * from pubs)), patents
where regexpr("(?:\D|\b)(\d{6,12})\b",middle) = appln_nr 
and (regexprmatches("european patent|patent application|patent office|ep patent|eu patent|patent",lower(context)) or 
    regexprmatches("\bEPO(?:\d|\b)",context) or regexprmatches ("\bEP\s*"||appln_nr,context)
    ) 
and (not regexprmatches("holotype|journal pone|journal pntd|paratype|scientometrics|specimen|dissection|\bnih\b|hepth|barcode|\bstrain|accession|\bbacter\b|patent ductus|patent foramen|arxiv|cern|biol|clin|letters|report",lower(context)))
and (not regexprmatches("[0-9]\."||appln_nr,middle) )

);


select jdict('documentId', docid, 'patentId', id, 'confidenceLevel', 0.8,'textsnippet',context)  from results where regexprmatches(authors,context);


