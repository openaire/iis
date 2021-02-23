create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());

hidden var 'prefixes_arrayexpress' from
select jmergeregexp(jgroup(prefix))
from ( select "\b"||regexpr("-",prefix,"[\s|\W|-|:|_|.]{0,1}")||'\d+' as prefix
         from ( select distinct c1 as prefix
                from ( select regexpr("\d", Accession,"") as c1
                        from arrayexpress_experiments)));


hidden var 'prefixes_ebi_ac_uk' from select '(?:(?:\b|[^A-Z])EGAD[\s|\W|-|:|_|.]{0,1}\d+)|(?:(?:\b|[^A-Z])EGAS[\s|\W|-|:|_|.]{0,1}\d+)|(?:(?:\b|[^A-Z])phs\d+[\s|\W|-|:|_|.]{0,1}v\d+[\s|\W|-|:|_|.]{0,1}p\d+)';


select jdict('documentId', docid, 'entity' , 'SRA', 'biomedicalId', regexpr('(?:(?:\b|[^A-Z])(SR[A-Z][:|-|_|.]{0,1}\d{6}))', middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid,textwindow2s(regexpr("\n",text," "), 10, 1, 10,'(?:(?:\b|[^A-Z])SR[A-Z][:|-|_|.]{0,1}\d{6})')
        from mydata);

select jdict('documentId', docid, 'entity', 'ArrayExpress', 'biomedicalId', regexpr("("||var('prefixes_arrayexpress')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('prefixes_arrayexpress'))
         from mydata);

select jdict('documentId', docid, 'entity', 'ebi_ac_uk', 'biomedicalId', regexpr("("||var('prefixes_ebi_ac_uk')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('prefixes_ebi_ac_uk'))
         from mydata);


select jdict('documentId', docid, 'entity', 'uniprot','biomedicalId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) from (setschema 'docid, prev, middle, next'select docid, textwindow2s(keywords(text),10,1,10,"\b([A-Z])([A-Z]|\d){5}\b") from mydata), uniprots where
middle = id and
(regexprmatches("\b\swiss\b|uniprot|swiss prot|uni prot|accession|sequence|protein",lower(prev||" "||middle||" "||next))
and not regexprmatches('\bFWF\b|\bARRS\b',(prev||" "||middle||" "||next))
);
