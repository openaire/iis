PRAGMA temp_store_directory = '.';

hidden var 'classCountLog' from select pyfun('math.log', cast((select count(*) from (select distinct taxonomy,class from taxonomies)) as float) ,2);

select jdict(
"documentId",
title,
"classes",
jdictgroupkey(
        jgroup(
            jdict(
            "classes", top3,
            "classLabels",top1,
            "confidenceLevel",p
            )
        )
        , "classes"
    )
)
from( 
select title,top3,top1,case when p < 0.04 then round(17.5*p,3) when p < 0.06 and p >= 0.04 then round(10*p+0.3,3) when p>=0.06 then round(min(3*p+0.72,0.99),3) end as p from (
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
                        from (setschema 'title, text' select jdictsplit(utf8clean(c1), 'id', 'abstract') from stdinput())
                    )
                )
                ,taxonomies
            where  (middle=term or regexpr('(\S+)(?:(\s)(\S+)|\s*$)',middle,'\1') = term)
        )
        group by title, taxonomy,class
    )
    group by title,c1
)))
group by title;



