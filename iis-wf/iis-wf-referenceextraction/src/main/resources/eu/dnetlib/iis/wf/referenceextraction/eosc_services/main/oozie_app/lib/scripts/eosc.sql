create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();


select jdict('documentid', id, 'name', name, 'textsnippet', j2s(prev,middle,next)) from 
(setschema 'id,prev,middle,next' select c1 as id,textwindow2s(regexpr("\n",c2," "),10,1,1,"https*://.*") from pubs), services 
where lower(regexpr("https*://(www\d*\.)*",stripchars(regexpr("(https*://.*)",middle),"\/,.;_()"),"")) =  url and not regexprmatches(".*/.*",next) group by id,name;