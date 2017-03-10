hidden var 'a' from (select jmergeregexp(jgroup(c1)) from (select * from egidigrams order by length(C1) desc)) ;
hidden var 'pos' from (select jmergeregexp(jgroup(c1)) from (select * from positives order by length(C1) desc)) ; 



create temp table results as 
select distinct doi,name,id,j2s(prevpack,middle,nextpack) as context,conf1,conf2 from (select  doi,c1,name,  id,             regexpcountwords(c2,keywords(j2s(prevpack,middle,nextpack))) as conf1,regexpcountuniquematches(var('pos'),keywords(j2s(prevpack,middle,nextpack))) as conf2,                    prevpack, middle, nextpack           
        from (             
            select doi, c1 as name,id, digrams as c2, lower(prev) as prevpack,lower(middle) as middle, middle as match, lower(next) as nextpack, c1 
            from (                 
                select * from (
                    setschema 'doi, prev, middle, next' select c1 as doi, textwindow2s(filterstopwords(lower(regexpr('\n',c2,' '))),10,3,3,var('a')) from(          
                        select * from (
                           setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput()                     
                        ) where c2 is not null 
                    )                 
                ), egidigrams where regexprmatches(c1,middle)             
                 )           
             )     
        ) where conf1+conf2>0;


select jdict('documentId', doi, 'conceptId',id,'confidenceLevel', 0.8) from (
select distinct doi,id from results where not regexprmatches("(?:\(2\d{3}\))|\[\d+\]|(?:et\sal)",context)) ;
