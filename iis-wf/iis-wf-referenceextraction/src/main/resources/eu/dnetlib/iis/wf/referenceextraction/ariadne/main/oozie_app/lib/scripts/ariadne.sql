PRAGMA temp_store_directory = '.';

create temp table pubs
as select * from (setschema 'id, text' select jdictsplit(c1, 'id', 'text') from stdinput()) where text <>'' and text not null;

select jdict('documentId', docid,'ariadneId', doi, 'langingPage', dsetID, 'confidenceLevel', sqroot(min(1.49,max(confidence))/1.5),'textsnippet',context) from
(
select docid, doi, dsetID, case when length(context) > 120 then round((length(titles) + conf*10)/(length(context)*1.0),2) else round(conf/10.0,2) end as confidence, context from (
select docid, doi, dsetID, regexpcountuniquematches(bag,lower(regexpr('\W|_',context,' '))) + 2*regexprmatches(creator,lower(context)) + regexprmatches(publisher,lower(context)) as conf, regexprmatches(titles,lower(context)) as match, titles, context
from
    (
    select docid, lower(stripchars(middle,'_')) as mystart,
    prev||' '||middle||' '||next as context   from
    (setschema 'docid,prev,middle,next' select id as docid, textwindow2s(normalizetext(textreferences(text)),15,3,15) from pubs)
    )
,titlesandtriples where mystart = words and match) where confidence > 0.28
union all
select docid, dsetID,doi,  0.8 as confidence,middle from
    ( setschema 'docid,middle' select id as docid, textwindow(regexpr("\n",text," "),0,0,1,"((?=.*[0-9])(?=.*[\D])(.{11,})|http.{10,})") as middle from pubs), ariadneurls
    where normalizetext(stripchars(middle,'.)(,[]\/')) = nurl
) group by docid, doi;
