drop table if exists patents;

create temp table jsoninp as select * from stdinput();

create table patents as 
    select c1 as appln_nr from 
        (setschema 'c1' select jsonpath(c1,"appln_nr") from (select * from jsoninp));

create index patentsindex on patents(appln_nr);