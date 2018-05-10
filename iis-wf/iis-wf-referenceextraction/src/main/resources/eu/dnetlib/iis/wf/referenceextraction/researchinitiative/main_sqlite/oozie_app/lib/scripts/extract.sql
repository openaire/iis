create temp table pubs as select * from 
(setschema 'id,importedauthors,year,text' select c1,jflatten(c2,c4,c5),c3,c6 from 
(setschema 'c1,c2,c3,c4,c5,c6'  select jsonpath(c1,'id','title','year','extractedAuthorFullNames','importedAuthors','text') from 
stdinput())) 
where year>2007 or year is null;

create temp table hep1 as select * from hep where c2 not in ("planck","compchem");
create temp table hep2 as select * from hep where c2 in ("planck","compchem");

create temp table results as 
select distinct id,year,c1 from  
(select id,year, jsplitv(importedauthors) as c17 from   (select pubs.* from pubs,hep 
where  importedauthors is not null and
(importedauthors like "%"||c2||"%" and importedauthors like "%collab%")
)
),hep1
where jlen(jset(regexprfindall("\b"||c2||"\b|collab",lower(c17))))>=2

union all


select distinct id, year, c1 from  
(setschema 'id,year,prev,middle,next' select id,year,textwindow2s(lower(text),15,3,8,"(?:planck|compchem) virtual organization") from pubs where text is not null), hep2  
where  regexprmatches("provid|work|service|support|national|egi|benefit|federation",j2s(prev,middle,next)) and hep2.c2 = regexpr("(planck|compchem)",middle);


delete from results where (year<2011 and c1 in ("icecube","pierre auger")) or  (year<2009 and c1 = "planck");

select jdict('documentId', id, 'conceptId',c1, 'confidenceLevel', 0.8) from results;
