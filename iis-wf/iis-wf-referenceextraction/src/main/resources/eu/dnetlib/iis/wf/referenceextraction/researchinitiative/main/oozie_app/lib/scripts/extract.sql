hidden var 'a' from (select jmergeregexp(jgroup(c1)) from (select * from egidigrams order by length(C1) desc)) ;
hidden var 'pos' from (select jmergeregexp(jgroup(c1)) from (select * from positives order by length(C1) desc)) ; 

select jdict('documentId', doi, 'conceptId',id,'confidenceLevel', conf) from (
    select distinct doi,id,1 as conf from (
        select  doi, c1, id,
	            regexpcountwords(c2,keywords(j2s(prevpack,middle,nextpack)))*0.4 + 
                    regexpcountwords(var('pos'),keywords(j2s(prevpack,middle,nextpack)))*0.4 as conf,   
                prevpack, middle, nextpack  
        from (
            select doi, id, digrams as c2, lower(prev) as prevpack,lower(middle) as middle, middle as match, lower(next) as nextpack, c1
            from (
                select * from (
                    setschema 'doi, prev, middle, next' select  c1 as doi, textwindow2s(filterstopwords(lower(regexpr('\n',c2,' '))),10,1,3,var('a')) from  (
                        select * from (
                            setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput()
                        ) where c2 is not null
                    )
                ), egidigrams where  regexprmatches(c1,middle)
            )
          )
    ) where conf>0
);
