create temp table jsoninp as select * from stdinput();

drop table if exists grants;
create table grants as select conceptId,conceptLabel,suggestedAcknowledgement
from 
(setschema 'conceptId,conceptLabel,suggestedAcknowledgement' 
          select c1 as conceptId,c2 as conceptLabel, c3 as suggestedAcknowledgement
                       from 
                          (select * from (setschema 'c1,c2,c3,c4' select jsonpath(c1, '$.id','$.label','$.suggestedAcknowledgement','$.jsonextrainfo') from jsoninp)));


create index grants_index on grants (conceptId,conceptLabel,suggestedAcknowledgement);