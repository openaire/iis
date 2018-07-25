PRAGMA temp_store_directory = '.';

create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();

select jdict('documentId', docid, 'projectId', 'CLARIN', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,1,10, '\bCLARIN\b') from pubs where c2 is not null
)) group by docid

union all

select jdict('documentId', docid, 'projectId', 'ANR', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,3,10, '(?:\bANR-\d{2}-\w{4}-\d{4}\b)|(?:\bFrance Life Imaging\b)') from pubs where c2 is not null
)) group by docid

union all

select jdict('documentId', docid, 'projectId', 'FLI-IAM', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,4,10, '(?:\bFrance Life Imaging\b)|(?:\bFLI-IAM\b)|(?:\bInformation Analysis and Management\b)') from pubs where c2 is not null
)) group by docid

union all

select jdict('documentId', docid, 'projectId', 'SDSN', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,2,10, '(?:\bSDSN\s)|(?:\bSDSN Greece\b)') from pubs where c2 is not null
)) group by docid

union all

-- Instruct-ERIC
select jdict('documentId', docid, 'projectId', 'Instruct-ERIC', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,2,10, '(?:\bInstruct-ERIC\b)|(?:\bESFRI\b)') from pubs where c2 is not null
) where regexprmatches('(?:\b[Aa]cknowledge)|(?:\bsupport\b)|(?:\bInstruct\b)|(?:\Landmark\b) ', context)
) group by docid

union all

-- ELIXIR-GR
select jdict('documentId', docid, 'projectId', 'ELIXIR-GR', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,1,10, '(?:\b5002780\b)|(?:\bELIXIR\b)') from pubs where c2 is not null
) where regexprmatches('(?:\bMIS\b)|(?:\bELIXIR-GR\b)|(?:\b[Ee]lixir-[Gg]r\b)|(?:\b[Ee]lixir\b[Gg]reece\b)', context)
) group by docid

union all

-- MIUR
select jdict('documentId', docid, 'projectId', 'MIUR', 'confidenceLevel', 1, 'match', middle, 'context', context) as C1 from (
select docid, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,1,20, '\bRBSI\d{2}\w{4}\b') from pubs where c2 is not null
)) group by docid;
