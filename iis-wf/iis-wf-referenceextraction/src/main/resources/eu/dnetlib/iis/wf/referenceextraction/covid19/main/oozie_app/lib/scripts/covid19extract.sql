create temp table pubs as select id,title, case when abstract is null then "" else abstract end as abstract, date1  from (setschema 'id,title,abstract,date1' select jsonpath(c1, '$.id', '$.title','$.abstract','$.date') from stdinput());

select jdict('id', id, 'title',title, 'date', date1) from (setschema 'id,title,abstract,date1' select * from pubs) 
where regexprmatches("covid(\s*)19|sars(\s*)cov(\s*)2|2019(\s*)ncov|hcov(\s*)19",regexpr("\-",lower(title),"")) or 
(regexprmatches("coronavirus|severe acute respiratory syndrome",regexpr(lower(title))) and regexprmatches("\bwuhan\b",regexpr(lower(title)))) or
regexprmatches("covid(\s*)19|sars(\s*)cov(\s*)2|2019(\s*)ncov|hcov(\s*)19",regexpr("\-",lower(abstract),"")) or 
(regexprmatches("coronavirus|severe acute respiratory syndrome",regexpr(lower(abstract))) and regexprmatches("\bwuhan\b",regexpr(lower(abstract))));