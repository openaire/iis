drop table if exists grants;

create table grants as select acronym,normalizedacro,case when fundingclass1="FCT" then acronym else grantid end as grantid,fundingclass1,fundingclass2,id from (setschema 'acronym,normalizedacro,grantid,fundingclass1,fundingclass2,id' select case when c1 is null then "UNKNOWN" else c1 end as acronym, case when c1 is not null then regexpr("[_\s]",normalizetext(lower(c1)),"[_\s]") else "unknown" end as normalizedacro, c3 as grantid,strsplit(c4,"delimiter:::") as fundingClass,c2 as id from (select * from (setschema 'c1,c2,c3,c4' select jsonpath(c1, '$.projectAcronym', '$.id' , '$.projectGrantId','$.fundingClass') from stdinput()) where regexprmatches("::",c4)));



CREATE INDEX grants_index on grants (grantid,normalizedacro,acronym,fundingClass1,fundingClass2,id);


