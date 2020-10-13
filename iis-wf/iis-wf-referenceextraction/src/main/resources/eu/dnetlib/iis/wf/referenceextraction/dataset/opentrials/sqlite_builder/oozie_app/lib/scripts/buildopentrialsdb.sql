drop table if exists opentrials;

create table opentrials as select id,acronym from 
(select id,publisher,case when alternateidentifiers is null then "" 
    else jsonpath(alternateidentifiers, publisher) end as acronym 
        from (setschema 'id,publisher,alternateIdentifiers' select jdictsplit(c1,"id","publisher","alternateIdentifiers") from stdinput())) ;

create index opentrialsindex on opentrials(acronym,id);



