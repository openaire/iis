PRAGMA temp_store_directory = '.';

create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();


select jsplitv('['||jdict('documentId', docid, 'conceptId', 'clarin', 'confidenceLevel', 0.5,'textsnippet',context)||','||jdict('documentId', docid, 'conceptId', 'dh-ch::subcommunity::2', 'confidenceLevel', 0.5,'textsnippet',context)||']') as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", textnoreferences(C2), " ")),10,3,10, '(?i)(?:\bCLARIAH\b)|(?:\bLINDAT\b)|(?:\bDLU\b)|(?:\bDutch Language Union\b)|(?:\bCELR\b)|(?:\bCLARINO\b)|(\bCLARIN\b)|(?:\bCLaDA-BG\b)') from pubs where c2 is not null
), grants where conceptLabel="CLARIN" and 
((regexprmatches('(?i)(\b(?:LINDAT)?/?CLARIN(?: Research)? Infrastructure\b)|(?:\b(?:LINDAT)?/?CLARIN repository\b)|(?:\brepository of (?:LINDAT)?/?CLARIN\b)|(?:\b(?:LINDAT)?/?CLARIN(?:[\./\s\-]D)? centre\b)', context)) or 
(regexprmatches('\b(?:LINDAT)?/?CLARIN\b',context) and regexprmatches('(?:\bCzech Republic\b)',context)) or 
(regexprmatches('\b(?:LINDAT)?/?CLARIN\b',context) and regexprmatches('(?:\bScience and Technology of the Portuguese Language\b)',context)) or 
(regexprmatches('(?:\bCLARIN[\./\s\-]CZ\b)|(?:\bCLARIN[\./\s\-]EU\b)|(?:\bCLARIN[\./\s\-]NL\b)|(?:\bCLARIN[\./\s\-]D\b)|(?:\bCLARIAH\b)|(?:\bCLARIN[\./\s\-]AT\b)|(?:\bLINDAT\b)|(?:\bCLARIN[\./\s\-]DK\b)|(?:\bDutch Language Union\b)|(?:\bCELR\b)|(?:\bCLARINO\b)|(?:\bCLARIN[\./\s\-]PL\b)|(?:\bSWE[\./\s\-]CLARIN\b)|(?:\bCLARIN[\./\s\-]LT\b)|(?:\bCLARIN[\./\s\-]PT\b)|(?:\bCLaDA-BG\b)|(?:\bCLARIN[\./\s\-]EL\b)|(?:\bCLARIN[\./\s\-]SI\b)|(?:\bCLARIN[\./\s\-]UK\b)|(?:\bCLARIN[\./\s\-]IT\b)|(?:\bFIN-CLARIN\b)|(?:\bCLARIN[\./\s\-]LV\b)|(?:\bHUN-CLARIN\b)|(?:\bCLARIN[\./\s\-]FR\b)|(?:\bHR-CLARIN\b)', context) and regexprmatches('(?i)(?:\bgrants?\b)|(?:\bfunding\b)|(?:\bfunded\b)|(?:\bprojects?\b)|(?:\bnational\b)', context)) or 
(regexprmatches('\bCLARIN[\./\s\-]SI\b',context) and regexprmatches('\bSlovenian\b',context)) or 
(regexprmatches('\bDLU\b', context) and regexprmatches('\bDutch\b', context)) or 
(regexprmatches('\b(?:LINDAT)?/?CLARIN\b', context) and regexprmatches('(?:\bfunding\b)|(?:\bgrant\b)|(?:\bH2020\b)', context) and regexprmatches('(?i)(?:\bCLARIN ERIC\b)|(?:\bEuropean Research Infrastructure for Language Resources and Technology\b)|(?:\b\d{6}\b)|(?:\bCLARIN-PLUS\b)|(?:\bCLARIN\+\b)|(?:\bEOSC-Hub\b)|(?:\bEUDAT2020\b)|(?:\bPARTHENOS\b)|(?:\bEuropeana DSI-1\b)|(?:\bEuropeana DSI-2\b)|(?:\bEuropeana DSI-3\b)',context)) and not regexprmatches('(?:\bfifa\b)|(?:\bworldcup\b)|(?:\bgene\b)|(?:www\.clarin\.com)|(?:\bCOFUND-CLARIN\b)|(?:\bLeopoldo Alas\b)|(?:\bLa connaissance\b)|(?:\bde la Tourette\b)', context))) group by docid

union all

select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5,'textsnippet',context) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),1,3,1, '(?:\bFrance Life Imaging\b)|(?:\bFLI-IAM\b)') from pubs where c2 is not null
), grants where conceptLabel="France Life Imaging") group by docid

union all

select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5,'textsnippet',context) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,2,10, '(?:\bSDSN\s)|(?:\bSDSN Greece\b)') from pubs where c2 is not null
), grants where conceptLabel="Environment and Economy" and (regexprmatches('(?i)(?:unsdsn.org)|(?:unsdsn.gr)|(?:IDDRI)|(?:Sustainable Development)|(?:United Nations)|(?:SDGs)', context) or regexprmatches('(?:\bUN\b)',context))) group by docid

union all

-- Instruct-ERIC
select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5,'textsnippet',context) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),10,1,10, '(?i)\bInstruct\b') from pubs where c2 is not null
), grants where conceptLabel="Instruct-ERIC" and 
regexprmatches('(?i)(?:\bERIC\b)|(?:\bESFRI\b)|(?:\bEuropean Strategy Forum on Research Infrastructures\b)|Instruct\-HiLIFE|Instruct\-FI|UK Instruct Centre|INSTRUCT platform|FRISBI|GRAL|Grenoble', context)
) group by docid

union all

-- ELIXIR-GR
select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5,'textsnippet',context) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(regexpr("\n", C2, " ")),20,1,10, '(?:\b5002780\b)|(?:\bELIXIR\b)') from pubs where c2 is not null
), grants where conceptLabel="ELIXIR-GR" and regexprmatches('(?:\bMIS\b)|(?:\bELIXIR-GR\b)|(?:\b[Ee]lixir-[Gg]r\b)|(?:\b[Ee]lixir\b[Gg]reece\b)', context)
) group by docid

union all

--EMBRC
select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5,'textsnippet',context) as C1 from (
select docid, prev||" "||middle||" "||next as context, conceptId  from
(setschema 'docid,prev,middle,next' select c1,
textwindow2s(textnoreferences(c2),10,7,10,"\bEMBRC\b|\bEMO BON\b|\bEMBRIC\b|((?i)european marine biological laboratories)|((?i)european marine biological resource cen)|((?i)european marine omics biodiversity observation network)")
from pubs where c2 is not null), grants where conceptLabel="EMBRC" and (regexprmatches("\bEMBRC\b|\bEMO BON\b|\bEMBRIC\b",prev||" "||middle||" "||next) or regexprmatches("((?i)european marine biological laboratories)|((?i)european marine biological resource cen)|((?i)european marine omics biodiversity observation network)",prev||" "||middle||" "||next))
) group by docid

union all

-- DARIAH
select jdict('documentId', docid, 'conceptId', conceptId, 'confidenceLevel', 0.5,'textsnippet',context) as C1 from (
select docid, conceptId, conceptLabel, stripchars(middle,'.)(,[]') as middle, prev||" "||middle||" "||next as context
from (
setschema 'docid,prev,middle,next' select c1, textwindow2s(keywords(filterstopwords(c2)),7,1,3, '\bDARIAH') from pubs where c2 is not null
), grants where conceptLabel="DARIAH EU" and (not regexprmatches("edariah",lower(middle)) and not regexprmatches("riyadh",lower(context)) )
) group by docid


union all

-- RISIS
select jdict('documentId', id, 'conceptId', 'RISIS', 'confidenceLevel', 0.8, 'textsnippet', prev||" <<< "||middle||" >>> "||next) from
(
select * from
(
-- cortext
select id, "CORTEXT" as ma, prev, middle, next from (setschema 'id,text,prev,middle,next' select id, text, textwindow2s(lower(text), 10,1,10, "(?:\b|\W)cortext(?:\b|\d)") from (setschema 'id,text' select c1,c2 from pubs)) where 
regexprmatches("cortext\.net|cortext\.org|www\.cortext\.|risis|ifris|text analysis|text mining|software|platform|plateforme|cortext manager|analysis|mining|nltk|github\.com\/cortext\/|corpus|\blisis\b|\b\inrae\b", prev||" "||middle||" "||next)
or regexprmatches("\bRISIS\b|\bINRAE\b|CorTexT|\bLISIS\b",text)

-- gate
union all

select id, "GATE" as ma, prev, middle, next   from (setschema 'id,prev,middle,next' select id, textwindow2s(text, 10,1,10, "\bGATE(?:\b|\d)|gatecloud|gate\.ac\.uk") from (setschema 'id,text' select c1,c2 from pubs)) 
where regexprmatches("text mining|gatecloud|gate\.ac\.uk|\buima\b|classifier|semantic|\bnlp\b|text engineering|natural language|language engineering|information extraction|text analytics|cunningham|text process|architecture text|maynard|tablan|bontcheva|gate framework|tokenizer|tokeniser|sheffield|text annotation|language processing|\bnltk\b|treetagger|\byatea\b", lower(prev||" "||middle||" "||next))


union all

select id, upper(regexpr("(orgreg|firmreg)",middle)) as ma, prev, middle, next from (setschema 'id,text,prev,middle,next' select id, text, textwindow2s(lower(text), 10,1,10, "\borgreg\b|\bfirmreg\b") from (setschema 'id,text' select c1,c2 from pubs)) 
where regexprmatches("\brisis\b", prev||" "||middle||" "||next) or regexprmatches("\bRISIS\b", text)



union all

select id, "RISIS" as ma, prev, middle, next from (setschema 'id,prev,middle,next' select id, textwindow2s(text, 10,1,10, "\bRISIS\b|\bRISIS1\b|\bRISIS2\b|\brisis\.eu\b") from (setschema 'id,text' select c1,c2 from pubs)) 
where (regexprmatches("recherche|patent|grant|support|acknowledge|innovation|research", prev||" "||middle||" "||next) and not regexprmatches("risis\.eu",lower(middle)) )
      or regexprmatches("risis\.eu",lower(middle))
) group by id) ;

