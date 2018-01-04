PRAGMA temp_store_directory = '.';

hidden var 'tara_unidentified' from select id from grants where fundingclass1="TARA" and grantid="unidentified" limit 1;

create temp table pubs as setschema 'c1,c2,c3,c4' select jsonpath(c1, '$.id', '$.text', '$.abstract', '$.title') from stdinput();

create temp table tara_match as 
select c1 as docid, id, 1 as confidence from (setschema 'c1,c4' select c1, c4 from pubs where c4 is not null), grants where tarakeywords!="" and regexprmatches('\b'||tarakeywords||'\b', lower(c4)) group by docid;
-- meta sto abstract
-- Einai ok omws na briskei mono ena
insert into tara_match 
select c1 as docid, id, 0.9 as confidence from (setschema 'c1,c3' select c1, c3 from pubs where c3 is not null and c1 not in (select distinct docid from tara_match)), grants where tarakeywords!="" and regexprmatches('\b'||tarakeywords||'\b', lower(c3)) group by docid;
-- meta to plh8os twn leksewn
-- Einai ok
insert into tara_match 
select c1 as docid, id, 0.8 as confidence from (
select c1, id, max(matches) as count from 
( select c1, id, regexpcountwords('\b'||tarakeywords||'\b', c2) as matches from (
setschema 'c1,c2' select c1, lower(comprspaces(regexpr('\n',c2,' '))) from pubs where c2 is not null and c1 not in (select distinct docid from tara_match)), grants where tarakeywords!="" and matches > 10
) group by c1
);
-- meta me acknowledgement
-- Einai ok
insert into tara_match 
select c1 as docid, case when count>0 then id else case when var('tara_unidentified') and generalmatch then var('tara_unidentified') else "" end end as id, 0.7 as confidence from (
select c1, id, max(matches) as count, regexpcountwords('(?:(?:\bfou?ndation)? tara expeditions?\b)|(?:\btara[ -]{1,2}(?:arctic|oceans?|pacific|med|girus|funding)\b)|(?:\btara transpolar drift\b)', prev||" "||middle||" "||next) as generalmatch from 
( select c1, id, regexpcountwords('\b'||tarakeywords||'\b', c2) as matches, textwindow2s(keywords(c2), 4,1,4, '(\b(?:tara)\b)') from (
setschema 'c1,c2' select c1, textacknowledgmentstara(lower(comprspaces(regexpr('\n',c2,' ')))) from pubs where c2 is not null and c1 not in (select distinct docid from tara_match)), grants where tarakeywords!=""
) group by c1
) where id;




select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5)) from tara_match group by docid;
