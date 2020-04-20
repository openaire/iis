hidden var 'pdbpos' from select jmergeregexp(jgroup(c1)) from (select * from positives order by length(C1) desc) ;
hidden var 'pdbneg' from select jmergeregexp(jgroup(c1)) from (select * from neg order by length(C1) desc) ;
hidden var 'chromeneg' from select jmergeregexp(jgroup(c1)) from (select * from chromeneg order by length(C1) desc) ;
hidden var 'dnaneg' from  select jmergeregexp(jgroup(c1)) from (select * from dnaneg order by length(C1) desc) ;

select jdict('documentId', pubid, 'conceptId', pdbid,'confidenceLevel',conf,'textsnippet',context) from (
select pubid,pdbid, case when conf=1 then 0.25 when conf=2 then 0.45 when conf = 3 then 0.55 when conf > 3 and conf<7 then 0.75 else 0.9 end as conf, context from
    (unindexed select *, regexpcountwords(var('pdbpos'),context) - regexpcountwords(var('pdbneg'),context) as conf from
        (select regexpr("(\b\d\w{3})",middle) as pdbid,pubid, j2s(prev1,prev2,prev3,prev4,prev5,prev6,prev7,prev8,prev9,prev10,middle,next1,next2,next3,next4,next5) as context from
            (setschema 'pubid,prev1,prev2,prev3,prev4,prev5,prev6,prev7,prev8,prev9,prev10,middle,next1,next2,next3,next4,next5' select c1 as pubid,textwindow(comprspaces(lower(filterstopwords(regexpr('[^\w-]',c2,' ')))) ,-10,5,'(?:-|^)\d\w{3}(?:-|$)') from (select * from (setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput()) where c2 is not null))
            where regexpr("(\b\d\w{3})",middle) in (select c1 from pdb)))

    where (pdbid != "3dna" or not regexprmatches(var('dnaneg'),context)) and
          (pdbid not like "%gal" and pdbid not like "%glc") and
          (pdbid not like "%kda" or not regexprmatches("molecular",context)) and
          ( (pdbid not like "%p%%" and pdbid not like "%q%%") or not regexprmatches(var('chromeneg'),context) ) and
          (pdbid not like "%hp%" or not regexprmatches("hairpin",context)) and
          (not regexprmatches("antibod|immunoglobulin",context) or pdbid not in (select * from antibodies) ) and
          (pdbid != "1914") and
          conf>0) ;
