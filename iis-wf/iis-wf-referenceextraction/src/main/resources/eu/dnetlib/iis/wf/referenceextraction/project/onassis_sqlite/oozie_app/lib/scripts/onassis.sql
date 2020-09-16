create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();
select  jdict('documentId', docid, 'prev', prev, 'middle', middle,'next',next)  from (setschema 'docid,prev,middle,next' select c1 as docid, textwindow2s(lower(keywords(comprspaces(c2))),10,2,3,"onassis foundation") from 
(setschema 'c1,c2' select * from pubs where c2 is not null));