PRAGMA temp_store_directory = '.';

create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();


select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),10,3,10, '(?i)(?:\bCLARIAH\b)|(?:\bCLARIN-NL\b)|(?:\bCLARIN-D\b)|(?:\bCLARIN\.SI\b)|(?:\bCLARIN-UK\b)|(?:\bLINDAT\b)|(?:\bCLARIN-CZ\b)|(?:\bCLARINO\b)|(?:\bCLARIN-IT\b)|(?:\bCLARIN-EU\b)|(?:\bCLARIN\b)|(?:\bDutch Language Union\b)') from pubs where c2 is not null
), grants where conceptLabel="CLARIN" and (regexprmatches('(?i)(?:\bCLARIN-NL\b)|(?:\bCLARIN-D\b)|(?:\bCLARIN[\.-]SI\b)|(?:\bCLARIN-UK\b)|(?:\bCLARIN-CZ\b)|(?:\bCLARIN-IT\b)|(?:\bCLARIN-EU\b)|(?:\bDutch Language Union\b)', context) or regexprmatches('(?:\bCLARIAH\b)|(?:\bLINDAT\b)|(?:\bCLARINO\b)|(?:\bCLARIN\b)', context)) and not regexprmatches('(?:\bfifa\b)|(?:\bworldcup\b)|(?:\bgene\b)|(?:www\.clarin\.com)|(?:\bCOFUND-CLARIN\b)|(?:\bLeopoldo Alas\b)|(?:\bLa connaissance\b)|(?:\bde la Tourette\b)', context)) group by docid

union all

select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),1,3,1, '(?:\bFrance Life Imaging\b)|(?:\bFLI-IAM\b)') from pubs where c2 is not null
), grants where conceptLabel="France Life Imaging") group by docid

union all

select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,2,10, '(?:\bSDSN\s)|(?:\bSDSN Greece\b)') from pubs where c2 is not null
), grants where conceptLabel="Environment and Economy" and (regexprmatches('(?i)(?:unsdsn.org)|(?:unsdsn.gr)|(?:IDDRI)|(?:Sustainable Development)|(?:United Nations)|(?:SDGs)', context) or regexprmatches('(?:\bUN\b)',context))) group by docid

union all

-- Instruct-ERIC
select jdict('documentId', docid, 'projectId', 'Instruct-ERIC', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),10,1,10, '(?i)\bInstruct\b') from pubs where c2 is not null
) where regexprmatches('(?i)(?:\bERIC\b)|(?:\bESFRI\b)|(?:\bEuropean Strategy Forum on Research Infrastructures\b)', context)
) group by docid

union all

-- ELIXIR-GR
select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,1,10, '(?:\b5002780\b)|(?:\bELIXIR\b)') from pubs where c2 is not null
), grants where conceptLabel="ELIXIR-GR" and regexprmatches('(?:\bMIS\b)|(?:\bELIXIR-GR\b)|(?:\b[Ee]lixir-[Gg]r\b)|(?:\b[Ee]lixir\b[Gg]reece\b)', context)
) group by docid;
