create temp table jsoninp as select * from stdinput();
update jsoninp set c1=regexpr('"jsonextrainfo":"{}"',c1,'"jsonextrainfo":"{\"keywords\":\"\"}"');

drop table if exists grants;
create table grants as select acronym, grantid, fundingclass1,fundingclass2,id,
	c1 as alias,
	case when fundingclass1="TARA" then
       case when grantid="unidentified" then "" else jsplitv(c2) end
       else "" end as tarakeywords
from 
(setschema 'acronym,grantid,fundingclass1,fundingclass2,id,c1,c2' 
          select case when c1 is null then "UNKNOWN" else c1 end as acronym, 
                 c3 as grantid,strsplit(c4,"delimiter:::") as fundingClass,c2 as id, 
                 jsonpath(c5,'$.alias','$.keywords')
                       from 
                          (select * from (setschema 'c1,c2,c3,c4,c5' select jsonpath(c1, '$.projectAcronym', '$.id' , '$.projectGrantId','$.fundingClass','$.jsonextrainfo') from jsoninp) 
                           where regexprmatches("::",c4)));

update grants set alias = "$a" where alias is null;

create index grants_index on grants (grantid,acronym,fundingclass1,fundingclass2,id,tarakeywords);