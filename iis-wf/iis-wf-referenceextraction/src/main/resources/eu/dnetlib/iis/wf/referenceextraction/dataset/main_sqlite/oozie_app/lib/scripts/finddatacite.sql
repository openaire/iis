PRAGMA temp_store_directory = '.';

create temp table docs
as select * from (setschema 'id, text' select jdictsplit(c1, 'id', 'text') from stdinput()) where text <>'' and text not null;

select jdict('documentId', docid, 'datasetId', dsetID, 'confidenceLevel', sqroot(min(1.49,max(confidence))/1.5)) from
(
select docid, dsetID, case when length(context) > 120 then round((length(titles) + conf*10)/(length(context)*1.0),2) else round(conf/10.0,2) end as confidence from (
select docid, dsetID, regexpcountuniquematches(bag,lower(regexpr('\W|_',context,' '))) + 2*regexprmatches(creator,lower(context)) + regexprmatches(publisher,lower(context)) as conf, regexprmatches(titles,lower(context)) as match, titles, context
from
    (
    select docid, lower(stripchars(middle,'_')) as mystart,
    prev||' '||middle||' '||next as context   from
    (setschema 'docid,prev,middle,next' select id as docid, textwindow2s(normalizetext(textreferences(text)),15,3,15) from docs)
    )
,titlesandtriples where mystart = words and match) where confidence > 0.28
union all
select docid, dsetID, 1 as confidence from
    ( setschema 'docid,middle' select id as docid, textwindow(comprspaces(filterstopwords(regexpr('(/|:)(\n)',text,'\1'))),0,0,'\b10.\d{4}/') from docs ),dois
    where normalizetext(stripchars(regexpr('(\b10.\d{4}/.*)',middle),'.,')) = normaldoi
) group by docid, dsetID;
