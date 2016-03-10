PRAGMA temp_store_directory = '.';

hidden var 'classCountLog' from select pyfun('math.log', cast((select count(*) from (select distinct taxonomy,class from taxonomies)) as float) ,2);

create temp table pubs as setschema 'title,text' select jsonpath(c1, '$.id', '$.abstract') from stdinput();


DROP TABLE IF EXISTS preclassifier;
create table preclassifier as 
select title, top3,top1,round(p,3) as p from (select title,top3,top1,round(((top2*1.0) / (wordcount * var('classCountLog'))),3) as p from (

setschema 'wordcount, title, top1, top2, top3'
    select wordcount,title,ontop(5,c3,c2,c3,c1) from
    (setschema 'wordcount, title, c1,c2,c3'
        select wordcount,title,taxonomy as c1, class as c2, sum(p) as c3 from
        (
            select * from
                (setschema 'wordcount, title, middle'
                    select regexpcountwords("\w+",text) as wordcount, title,textwindow(text,0,1,1)
                    from (
                        select title, stem(filterstopwords(keywords(text))) as text
                        from pubs
                    )
                )
                ,taxx
            where  (middle=term or regexpr('(\S+)(?:(\s)(\S+)|\s*$)',middle,'\1') = term)
        )
        group by title, taxonomy,class
    )
    group by title,c1
)) where p >= 0.1;


select jdict(
"documentId",
T.title,
"classes",
jdictgroupkey(
        jgroup(
            jdict(
            "classes", T.top3,
            "classLabels",T.top1,
            "confidenceLevel",T.p
            )
        )
        , "classes"
    )
)
from (
select title ,top3,top1,min(round(p,3),0.99) as p from (
select title,top3,top1,
case when top3 = "DDClasses" then case when p<0.05 then 12 * p when p<0.12 then 2.85*p + 0.457 else 2.375*p+0.515 end
     when top3 = "ACMClasses" then case when p<0.02 then 30 * p when p<0.04 then 10*p + 0.4 else 19*p+0.04 end
     when top3 = "arXivClasses" then case when p<0.03 then 20 * p when p<0.04 then 20*p else 4.75*p+0.61 end
     when top3 = "meshEuroPMCClasses" then case when p<0.02 then 20 * p when p<0.03 then 20*p + 0.2 else 9.5*p+0.515 end 
end as p from (
select title,top1,top3, min(round(
                (top2*1.0 / (wordcount * var('classCountLog'))
                )
                 ,3
                ),1.0)/1.0 as p from
(
setschema 'wordcount, title, top1, top2, top3'
    select wordcount,title,ontop(5,c3,c2,c3,c1) from
    (setschema 'wordcount, title, c1,c2,c3'
        select wordcount,title,taxonomy as c1, class as c2, sum(p) as c3 from
        (
            select * from
                (setschema 'wordcount, title, middle'
                    select regexpcountwords("\w+",text) as wordcount, title,textwindow(text,0,1,1)
                    from (
                        select title, stem(filterstopwords(keywords(text))) as text
                        from pubs
                    )
                )
                ,taxonomies
            where  (middle=term or regexpr('(\S+)(?:(\s)(\S+)|\s*$)',middle,'\1') = term)
        )
        group by title, taxonomy,class
    )
    group by title,c1
)))) T , preclassifier where T.title = preclassifier.title and T.top3 = preclassifier.top1 and T.p >= 0.80
group by T.title;



