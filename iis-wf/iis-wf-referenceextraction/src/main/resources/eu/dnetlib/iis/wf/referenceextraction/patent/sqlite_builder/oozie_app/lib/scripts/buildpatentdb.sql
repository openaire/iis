drop table if exists patents;

create temp table jsoninp as select * from stdinput();


create table patents as select c1,c2,normal,c4,c5
from 
(setschema 'c1,c2,normal,c4,c5' 
          select c1 as c1, c2 as c2, normal as normal, c4 as c4, c5 as c5
                       from 
                          (select * from (setschema 'c1,c2,normal,c4,c5' select jsonpath(c1, 'c1','c2','normal','c4','c5') from jsoninp)));
-- this is minimalistic input, required by the mining script, we are not interested in the rest of the fields, so we can ignore them

create index patents_norm on patents(normal, c2, c4, c1);