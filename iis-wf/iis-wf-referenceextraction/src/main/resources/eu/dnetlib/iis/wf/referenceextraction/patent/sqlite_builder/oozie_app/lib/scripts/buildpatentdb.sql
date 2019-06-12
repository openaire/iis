drop table if exists patents;

create temp table jsoninp as select * from stdinput();

create table patents as 
    select c1 as appln_id, c2 as appln_auth, c3 as appln_nr, c4 as appln_nr_epodoc from 
        (setschema 'c1,c2,c3,c4' select jsonpath(c1,"appln_id","appln_auth","appln_nr","appln_nr_epodoc") from (select * from jsoninp));

create index patentsindex on patents(appln_nr, appln_auth, appln_id, appln_nr_epodoc);