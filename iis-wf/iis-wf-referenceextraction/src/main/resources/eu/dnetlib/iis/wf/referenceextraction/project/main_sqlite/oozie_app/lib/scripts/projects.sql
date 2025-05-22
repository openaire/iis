PRAGMA temp_store_directory = '.';

hidden var 'fp7pos' from select jmergeregexp(jgroup(c1)) from (select * from fp7positives order by length(C1) desc) ;
hidden var 'fp7negheavy' from select jmergeregexp(jgroup(c1)) from (select * from fp7strongfilterwords order by length(C1) desc);
hidden var 'fp7neglight' from select jmergeregexp(jgroup(c1)) from (select * from fp7weakfilterwords order by length(C1) desc);
hidden var 'fp7pospos' from select jmergeregexp(jgroup(c1)) from (select * from fp7pospos order by length(C1) desc);
hidden var 'fp7middlepos' from select jmergeregexp(jgroup(c1)) from (select * from fp7positives union select * from fp7pospos union select * from fp7middlepos);
hidden var 'wtnegheavy' from select jmergeregexp(jgroup(c1)) from (select * from wtstrongfilterwords order by length(C1) desc);
hidden var 'wtneglight' from select jmergeregexp(jgroup(c1)) from (select * from wtweakfilterwords order by length(C1) desc);
hidden var 'wtpospos' from select jmergeregexp(jgroup(c1)) from (select * from wtposposwords order by length(C1) desc);
hidden var 'nihposshort' from select jmergeregexp(jgroup(word)) from (select * from nihposnamesshort order by length(word) desc);
hidden var 'nihposfull' from select jmergeregexp(jgroup(word)) from (select * from nihposnamesfull order by length(word) desc);
hidden var 'nihpositives' from select jmergeregexp(jgroup(word)) from (select * from nihpositives order by length(word) desc);
hidden var 'nihnegatives' from select jmergeregexp(jgroup(word)) from (select * from nihnegatives order by length(word) desc);
hidden var 'hfripos' from select  "(?:innovation project)|(?:multigold numbered)|(?:fellowship number)|(?:grant fellowship)|(?:innovation grant)|(?:scholarship code)|(?:technology gsrt)|(?:project number)|(?:faculty grant)|(?:hfri project)|(?:elidek grant)|(?:agreement no)|(?:funded grant)|(?:project no)|(?:hfri grant)|(?:gsrt grant)|(?:hfri fm17)|(?:hfri code)|(?:grant no)|(?:grant ga)|(?:ga hfri)";
hidden var 'hfrineg' from select  "(?:\bekt\b)|(?:eliamep)|(?:\bforth\b)|(?:\bi.k.a.\b)|(?:\bipep\b)";
hidden var 'miur_unidentified' from select id from grants where fundingclass1="MIUR" and grantid="unidentified" limit 1;
hidden var 'wt_unidentified' from select id from grants where fundingclass1="WT" and grantid="unidentified" limit 1;
hidden var 'gsri_unidentified' from select id from grants where fundingclass1="GSRI" and grantid="unidentified" limit 1;
hidden var 'cihr_unidentified' from (select id from grants where fundingclass1="CIHR" and grantid="unidentified" limit 1);
hidden var 'nserc_unidentified' from (select id from grants where fundingclass1="NSERC" and grantid="unidentified" limit 1);
hidden var 'sshrc_unidentified' from (select id from grants where fundingclass1="SSHRC" and grantid="unidentified" limit 1);
hidden var 'nrc_unidentified' from (select id from grants where fundingclass1="NRC" and grantid="unidentified" limit 1);
hidden var 'inca_unidentified' from (select id from grants where fundingclass1="INCa" and grantid="unidentified" limit 1);
hidden var 'hfri_unidentified' from (select id from grants where fundingclass1="HFRI" and grantid="unidentified" limit 1);
hidden var 'irc_unidentified' from (select id from grants where fundingclass1="IRC" and grantid="unidentified" limit 1);
hidden var 'hrb_unidentified' from (select id from grants where fundingclass1="HRB" and grantid="unidentified" limit 1);


create  temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();

create temp table incaprojects as 
select id, grantid, gid, jmergeregexp(terms) as terms, jlen(terms) as lt from 
   (select id, grantid, s2j(keywords(regexpr("\d", terms, ""))) as terms, regexpr("\s+",gid,"\s*") as gid from 
       (select id, grantid, regexpr("(\D+)",grantid) as terms, regexpr("\D([\d|\s]+)$",grantid) as gid from grants 
        where fundingclass1 = "INCa" and gid is not null));

create temp table hfri_unidentified_only as select docid, var('hfri_unidentified') as id, prev, middle, next from (setschema 'docid,prev,middle,next' 
   select c1 as docid, textwindow2s(filterstopwords(lower(c2)), 10,3,10, "\bhfri\b|h\.f\.r\.i\.|hellenic foundation research|greek foundation research|ελιδεκ|ελληνικο ιδρυμα ερευνας|ελληνικό ίδρυμα έρευνας|elidek") from ((setschema 'c1,c2' select * from pubs where c2 is not null))) 
   where var('hfri_unidentified') and lower(j2s(prev,middle,next)) not like "%himalayan%" and not regexprmatches(var('hfrineg'), lower(j2s(prev,middle,next))) and regexprmatches("gsrt|greek|greece|hellenic",lower(j2s(prev,middle,next)));

create temp table hrb_unidentified_only as select docid, var('hrb_unidentified') as id, prev, middle, next from  (select c1 as docid, prev,middle,next from (setschema 'c1,prev,middle,next' select c1, textwindow2s(lower(c2),10,3,10,'health research board') from (setschema 'c1,c2' select * from pubs where c2 is not null)) 
   where var('hrb_unidentified') and (regexprmatches("project|grant|fund|co-fund|award|received|recipient|acknowledg|support|fellow|student|scholar|partner|financ|assist|sponsor|endorse|contract|grateful|initiative|trial",j2s(prev,middle,next)) 
   and not regexprmatches("commonwealth|public health research|industrial health research|\bihrb\b",j2s(prev,middle,next))));

create temp table output_hfri as 
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', textsnippet_) as C1, docid, id from (
 select  docid, id, textsnippet_ from 
  (setschema 'docid,textsnippet_,textwin1,textwin2,textwin3, proj_id, id'  select docid, textsnippet_, textwindow2s(textsnippet_,2,1,1,"(?:\b|fm17)"||proj_id||"\b") as textwin, proj_id, id from ( 
   select docid,t2.Project_id as proj_id, t2.id,  t1.textsnippet_   from
    ( setschema 'docid, textsnippet_, res' select distinct  docid, textsnippet_, jsplitv(regexprfindall('(?:\b|fm17)(\d{2,4})\b',textsnippet_)) as res
      from ( select docid, filterstopwords(lower(keywords(j2s(prev,middle,next)))) as textsnippet_ from  (setschema 'docid, id, prev, middle, next' select * from hfri_unidentified_only)) ) as t1, 
      (select id, grantid as project_id from grants where fundingclass1 = "HFRI") as t2  where res = t2.project_id and t2.project_id is not null
    )
  ) where (regexprmatches(var('hfripos'),textwin1) or textwin2 like "%"||"fm17"||proj_id||"%" or  textwin2 = "."||proj_id) and textwin1 is not null and  textwin2 is not null 
    and   textwin3 is not "hellenic" group by docid, id);
    


create temp table matched_undefined_miur_only as select distinct docid, var('miur_unidentified') as id, prev,middle,next from (setschema 'docid,prev,middle,next'
select c1 as docid, textwindow2s(c2,10,1,10, '\b(?:RBSI\d{2}\w{4})\b') from (setschema 'c1,c2' select * from pubs where c2 is not null)) 
where var('miur_unidentified') and (regexprmatches('\b(?:RBSI\d{2}\w{4})\b', middle));

create temp table matched_undefined_irc_only as select distinct docid, var('irc_unidentified') as id, prev,middle,next from (setschema 'docid,prev,middle,next'
select c1 as docid, textwindow2s(keywords(comprspaces(lower(regexpr("\n",c2," ")))),10,3,10, 'irish research council') from (setschema 'c1,c2' select * from pubs where c2 is not null)) 
where var('irc_unidentified');




create temp table matched_undefined_inca_only as select distinct docid, var('inca_unidentified') as id, prev,middle,next from (setschema 'docid,prev,middle,next'
select c1 as docid, textwindow2s(c2,15,4,10, '\bINCa|French National Cancer Institute') from (setschema 'c1,c2' select * from pubs where c2 is not null))
where var('inca_unidentified') 
and regexprmatches("fund|grant|inserm|dgos|support|project|fondation|program|financial|plbio|acknowledge|thank|award|finance|financ√©e|subvention|bourse|aide|soutien|remerci|recipient|\bprojet\b",lower(j2s(prev,middle,next))) 
and not regexprmatches("instituto nacional câncer|brazil|janeiro|mexico|méxico|instituto nacional cancerología", lower(j2s(prev,middle,next))) 
and not regexprmatches("incan(\D|\s)",lower(j2s(prev,middle,next)));



create temp table matched_undefined_wt_only as select distinct docid, var('wt_unidentified') as id, prev,middle,next from (setschema 'docid,prev,middle,next'
select c1 as docid, textwindow2s(c2,20,2,3, '(\bWel?lcome Trust\b|\bWT\b)') from (setschema 'c1,c2' select * from pubs where c2 is not null)) where var('wt_unidentified') and (regexprmatches('\bWel?lcome Trust\b', middle) or 
regexpcountwords('(?:\bwell?come trust\b)|(?:(?:\bthis work was|financial(?:ly)?|partial(?:ly)?|partly|(?:gratefully\s)?acknowledges?)?\s?\b(?:support|fund|suppli?)(?:ed|ing)?\s(?:by|from|in part\s(?:by|from)|through)?\s?(?:a)?\s?(?:grant)?)|(?:(?:programme|project) grant)|(?:(?:under|through)?\s?(?:the)?\s(?:grants?|contract(?:\snumber)?)\b)|(?:\bprograms? of\b)|(?:\bgrants? of\b)|(?:\bin part by\b)|(?:\bthis work could not have been completed without\b)|(?:\bcontract\b)|(?:\backnowledgments?\b)', lower(prev||' '||middle||' '||next)) > 3);




create temp table matched_undefined_gsri as select c1 as docid, var('gsri_unidentified') as id, prev,middle,next from  
(setschema 'c1, prev, middle, next' select c1, 
textwindow2s(keywords(c2), 10,6,10,'(?i)\bGSRT\b|\bΓΓΕΤ\b|(?:greek|general) secretariat (?:for|of) research|\bKRIPIS\b|γενικ(?:ή|η) γραμματε(?:ί|ι)α (?:έ|ε)ρευνας και τεχνολογ(?:ί|ι)ας|GENERAL SECRETARIAT (?:FOR|OF) RESEARCH AND TECHNOLOGY|GREEK SECRETARIAT (?:FOR|OF) RESEARCH') 
from pubs  where c2 is not null)  
where var('gsri_unidentified') is not null and
regexprmatches("(?i)greece|greek|foundation|grant|project|\bpavet\b|funded|hellenic|supported|acknowledge|\bgr\b|research|program|secretariat|γραμματε(?:ί|ι)α",prev||" "||middle||" "||next)  
and not (regexprmatches("(?i)spanish|\bmineco\b|\bspain\b",prev||" "||middle||" "||next) and not regexprmatches("(?i)hellenic|\bgreece\b|\bgreek\b|\bgsrt\b",prev||" "||middle||" "||next) )
group by docid;

create temp table intermediate_canadian_results as
select docid, (prev||" "||middle||" "||next) as temp_textsnippet, (prev||" <<< "||middle||" >>> "||next) as textsnippet
from (setschema 'docid,prev,middle,next' select c1, textwindow2s(filterstopwords(keywords(c2)), 15,1,15, "^(?:(?:(?:CIHR|IRSC)|(?:NSERC|CRSNG)|(?:SSHRC|CRSH|SSRCC))|(?i)(?:co(?:(?:un(?:cil|sel))|(?:nseil(?:s)?))|canad(?:a|ian)))$")
    from (select * from pubs where (var('cihr_unidentified') is not null or var('nserc_unidentified') is not null or var('sshrc_unidentified') is not null or var('nrc_unidentified') is not null))
    where c2 is not null)
where
( /* Terms */
    /* Acronyms */
    regexprmatches("^(?:CIHR|(?:NSERC|CRSNG)|(?:SSHRC|SSRCC))$", middle)
    or (
        regexprmatches("^IRSC$", middle) /* This is the french acronym of CIHR. It also refers to some other organizations, so we search and exclude them. */
        and not regexprmatches(".*(?:informal relationships social capital|interlocus sexual conflict|international (?:rosaceae|rosbreed) snp consortium|iranian (?:seismological|remote sensing) cent(?:er|re)).*", lower(prev||" "||next))
    )
    or (
        regexprmatches("^CRSH$", middle) /* This is the french acronym of SSHRC. It also refers to some other organizations, so we search and exclude them. */
        and not regexprmatches(".*(?:coalition restaurant health safety).*", lower(prev||" "||next))
    )
    or (/* Full-names */
        (   /* Middle: "Council", "Counsel", "Conseil", "Conseils" --> NSERC/CRSNG, SSHRC/CRSH/SSRCC, NRC */
            regexprmatches("^co(?:(?:un(?:cil|sel))|(?:nseil(?:s)?))$", lower(middle))
            and (
                -- The "middle" at the beginning of the fullname.
                (   regexprmatches("^recherche(?:s)?(?:(?:\s(?:g(?:e|é)nie|science(?:s)?)(?:\s(?:humaines|naturel(?:les)?)?)?(?:\sg(?:e|é)nie)?)?)?\scanada.*", lower(next))    -- The term "canada" is put as mandatory here and below, as we get false-positives.
                    or regexprmatches("^(?:social\sscience(?:s)?\shumanities\sresearch|national\srecherche(?:s)?)\scanada.*", lower(next))
                )
                or ( -- The "middle" at the end of the fullname.
                    ( regexprmatches(".*(?:social|nat(?:ural|ional))\sscience(?:s)?\s(?:(?:engineering|humanities)(?:\sresearch)?|research)$", lower(prev))
                        or regexprmatches(".*(?:humanities|national)\sresearch$", lower(prev))  -- Here we cover the full-name of NRC and a variation of SSHRC.
                    )
                    and (   -- Add this just to be more sure it's a "canadian" match..
                            regexprmatches("^canada.*", lower(next))
                            or regexprmatches(".*canada\s(?:social|nat(?:ural|ional)).*", lower(prev))
                        )
                    )
                )
            )
            or (    /* Middle: "Canada", "Canadian" --> CIHR/IRSC */
                regexprmatches("^canad(?:a|ian)$", lower(middle))   -- "Canadian" match for sure.
                and ( -- The "middle" at the beginning of the fullname
                    regexprmatches("^institute(?:s)?\shealth\sresearch.*", lower(next))
                    or -- The "middle" at the end of the fullname
                    regexprmatches(".*institut(?:(?:e)?(?:s)?)?\srecherche\ssant(?:e|é)$", lower(prev))
                )
            )
        )
    )
and (   /* Relation */
    regexprmatches(".*(?:fund|support|financ|proje[c]?t|grant|subvention|sponsor|parrain|souten|subsidiz|promot|acquir|acknowledg|administ|assist|donor|bailleur|g(?:e|é)n(?:e|é)rosit).*", lower(prev||" "||next))
    or regexprmatches(".*(?:thank|gratefull|(?:re)?merci).*", lower(prev))
)
order by docid;  -- The multiple_canadian_funders-UDF is depending on the docid-ordered data.

create temp table output_table as
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingclass1, grantid from (
select docid,id,fundingclass1, grantid,prev,middle,next from (select * from (setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(regexpr("\n",c2," "),10,1,10, '(?:RBSI\d{2}\w{4})|(?:2015\w{6})') from (setschema 'c1,c2' select * from pubs where c2 is not null) ) ,grants where fundingclass1="MIUR" and regexpr("((?:RBSI\d{2}\w{4})|(?:2015\w{6}))",middle) = grantid)
group by docid,id)

union all

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5), 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingclass1, grantid from
( select prev,middle,next,docid,id,max(confidence) as confidence, docid, id,  fundingclass1, grantid from ( select 
(0.3
+hardmatch*0.6
+subrcukmatch
+rcukmatch)
as confidence, docid, id,prev,middle,next, fundingclass1, grantid
from (
unindexed select regexprmatches('\bRCUK\b|\bUKRI\b|[Rr]esearch [Cc]ouncils UK', context) as rcukmatch,
regexprmatches('\b'||rcuk_subfunder||'\b', context) as subrcukmatch,
regexprmatches('(?:G\d{6,7})|(?:[A-Z]{2}\/\w{6,7}\/\d{1,2}(?:\/xx)?)|(?:(?:BBS|PPA)\/[A-Z](?:\/[A-Z])?\/(?:\w{8,9}|(?:\d{4}\/)?\d{5}(?:\/\d)?))|(?:(?:RES|PTA)-\d{3}-\d{2}-\d{4}(?:-[A-Z]|-\d{1,2})?)|(?:MC_(?:\w{8,10}|\w{2}_(?:\w{2,4}_)?(?:\d{4,5}|[UG]\d{7,9})(?:\/\d{1,2})?))|(?:MC_\w{2}_\w{2}(?:_\w{2})?\/\w{7}(?:\/\d)?)|(?:ESPA-[A-Z]{3,6}-\d{4}(?:-[A-Z]{3}-\d{3}|-\d{3})?)', middle) as hardmatch,
docid, id, fundingclass1, grantid,middle,prev,next
from (
select docid, stripchars(middle,'.)(,[]') as middle, prev, next, prev||' '||middle||' '||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(c2,15,1,1,'(?:[SG]?\d{5,7}(?:\/\d)?)|(?:[A-Z]{2}\/\w{6,7}\/\d{1,2}(?:\/xx)?)|(?:[GE]\d{2,3}\/\d{1,4})|(?:(?:BBS|PPA)\/[A-Z](?:\/[A-Z])?\/(?:\w{8,9}|(?:\d{4}\/)?\d{5}(?:\/\d)?))|(?:(?:RES|PTA)-\d{3}-\d{2}-\d{4}(?:-[A-Z]|-\d{1,2})?)|(?:MC_(?:\w{8,10}|\w{2}_(?:\w{2,4}_)?(?:\d{4,5}|[UG]\d{7,9})(?:\/\d{1,2})?))|(?:MC_\w{2}_\w{2}(?:_\w{2})?\/\w{7}(?:\/\d)?)|(?:[A-Za-z]{3,9}\d{5,7}a?)|(?:ESPA-[A-Z]{3,6}-\d{4}(?:-[A-Z]{3}-\d{3}|-\d{3})?)') from pubs where c2 is not null
)), grants
WHERE fundingclass1="UKRI" and middle = grantid
) where confidence>0.3 ) group by docid,id)

union all
--DFG
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingclass1, grantid from
(setschema 'docid,prev,middle,next' select c1, textwindow2s(filterstopwords(keywords(c2)),10,2,7,"\w{3}\s\d{1,4}") from pubs where c2 is not null), grants
where lower(regexpr("\b(\w{3}\s\d{1,4})\b",middle)) = grantid and
regexprmatches("support|project|grant|fund|thanks|agreement|research|acknowledge|centre|center|nstitution|program|priority|dfg|german|dutch|deutche",lower(prev||" "||next)) group by docid, id
--DFG

union all
--inca 
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, "INCa", grantid  from 
    (select * from (setschema 'docid,prev,middle,next' select c1, textwindow2s(filterstopwords(c2), 10,2,5, "INCa\-\w+(?:\-|\s|$|\b)") from pubs where c2 is not null), incaprojects 
         where regexprmatches("\b"||gid||"\b",j2s(prev,middle,next)) and (regexpcountuniquematches(lower(terms), lower(j2s(prev,middle,next))) = lt or regexpcountuniquematches(lower(terms), lower(j2s(prev,middle,next)))>1)) 
group by docid,id


union all
-- CHIST-ERA
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingclass1, grantid from
(setschema 'docid,prev,middle,next' select c1, textwindow2s(comprspaces(lower(keywords(c2))),10,2,10,"\bchist era\b") from pubs where c2 is not null), grants
where (regexprmatches("\b"||keywords(lower(acronym))||"\b", prev||" "||middle||" "||next) or grantid = "unidentified") and fundingclass1 = "CHIST-ERA"  group by docid, id

union all
-- Canadian funders
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', textsnippet) as C1, docid, id, "Canadian", "unidentified"
from (
      multiple_canadian_funders select *, var('cihr_unidentified'), var('nserc_unidentified'), var('sshrc_unidentified'), var('nrc_unidentified') from intermediate_canadian_results
) group by docid, id


union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingclass1, grantid from (
select docid,id,fundingclass1,grantid,prev,middle,next from (select docid,id,grantid,middle,fundingclass1,grantid,prev,middle,next  from (
setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(c2,15,1,5,"(?:\bANR-\d{2}-\w{4}-\d{4}\b)|\b(?:06|07|10|11|12|13|14|15|16|17|18|19)\-\w{4}\-\d{4}(?:\-\d{2})*\b|(.+\/\w+\/\d{4}\W*\Z)|(\d{4})|(\d{2}-\d{2}-\d{5})|(\d{6,7})|(\w{2}\d{4,})|(\w+\/\w+\/\w+)|(\w*\/[\w,\.]*\/\w*)|(?:\d{3}\-\d{7}\-\d{4})|(?:(?:\b|U)IP\-2013\-11\-\d{4}\b)|(\b(?:(?:(?:\w{2,3})(?:\.|\-)(?:\w{2,3})(?:\.|\-)(?:\w{2,3}))|(?:\d+))\b)|(?:\b\d{7,8}\b)|(?:\b\d{3}[A-Z]\d{3}\b)|(?:[A-Z]{2,3}.+)|(?:\d{4}\-\w+\-\w+(\-\d+)*)|(?:\d{4}\-\d{2,})") from (setschema 'c1,c2' select * from pubs where c2 is not null) ) , grants where 
(upper(regexpr("(\w+.*\d+)",middle)) = grantid and (fundingclass1 in ("FCT","ARC"))) or 
(regexpr("(\d{5,7})",middle)=grantid and fundingclass1 = "NHMRC" and regexprmatches("nhmrc|medical research|national health medical",filterstopwords(normalizetext(lower(j2s(prev,middle,next)))))) or 
(regexpr("(\w*\/[\w,\.]*\/\w*)",middle)=grantid and fundingclass1 = "SFI") or
(regexpr("\b((?:06|07|10|11|12|13|14|15|16|17|18|19)\-\w{4}\-\d{4}(?:\-\d{2})*)\b",middle)=grantid and fundingclass1 = "ANR") or
(regexpr("\b(ANR-\d{2}-\w{4}-\d{4})\b",middle)=grantid and fundingclass1 = "ANR") or
( regexpr("(\d+)",middle)=grantid and fundingclass1 = "CONICYT" and regexprmatches("conicyt|fondecyt",lower(j2s(prev,middle,next)) )  ) or 
( regexpr("(\b\d{3}[A-Z]\d{3}\b)",middle)=grantid and fundingclass1 = "TUBITAK" and regexprmatches("tubitak|tubitek|tbag|turkey|turkish|\btub\b|\bbitak\b|\bitak\b|tub|ubitak|tu bi tak|tubtak|itak|project",lower(j2s(prev,middle,next)) )  ) or
( stripchars(regexpr("([A-Z]{2,3}.+)",middle),"[]\().{}?;") = grantid and fundingclass1 = "SGOV") or
(regexpr("(\d{4}\-\w+\-\w+(?:\-\d+)*)",middle) = grantid and fundingclass1="INNOVIRIS") or
(regexpr("(\d{4}\-\d+)",regexpr("\-ANTICIPATE\-|\-ATTRACT\-|\-PRFB\-|\-BB2B\-",middle,"-")) = grantid and fundingclass1="INNOVIRIS" and (regexprmatches("innoviris|prfb",lower(j2s(prev,middle,next))) or regexprmatches("anticipate$|attract$",lower(prev))) and regexprmatches("\banticipate\b|\battract\b|prfb|\bbb2b\b",lower(j2s(prev,middle,next))) )  or
(regexpr("(\d{2}-\d{2}-\d{5})",middle) = grantid and fundingclass1 = "RSF" and ((regexpcountwithpositions("(?:russian science foundation)|(?:russian science fund)|(?:russian scienti)|(?:russian scientific foundation)|(?:russian scientific fund)|(?:scientific foundation of russian federation)|(?:rsf)|(?:rusiina scientific foundation)|(?:russ. sci. found.)|(?:russan science foundation)|(?:russia science foundation)|(?:russian academic fund)|(?:russian federation)|(?:russian federation foundation)|(?:russian foundation for sciences)|(?:russian foundation of sciences)|(?:russian fundamental research foundation)|(?:russian research foundation)|(?:russian scence foundation)|(?:russian sci. fou.)|(?:russian science)|(?:rnf)|(?:russian national foundation)|(?:rnsf)|(?:russian national science foundation)|(?:rscf)|(?:rscif)|(?:rsp)|(?:rcf)",lower(prev)) + regexpcountwithpositions("(?:russian science foundation)|(?:russian science fund)|(?:russian scienti)|(?:russian scientific foundation)|(?:russian scientific fund)|(?:scientific foundation of russian federation)|(?:rsf)|(?:rusiina scientific foundation)|(?:russ. sci. found.)|(?:russan science foundation)|(?:russia science foundation)|(?:russian academic fund)|(?:russian federation)|(?:russian federation foundation)|(?:russian foundation for sciences)|(?:russian foundation of sciences)|(?:russian fundamental research foundation)|(?:russian research foundation)|(?:russian scence foundation)|(?:russian sci. fou.)|(?:russian science)|(?:rnf)|(?:russian national foundation)|(?:rnsf)|(?:russian national science foundation)|(?:rscf)|(?:rscif)|(?:rsp)|(?:rcf)",lower(next),1))  - (regexpcountwithpositions("RFBR|Basic|BASIC",next,1) + regexpcountwithpositions("RFBR|Basic|BASIC",prev)) >= 0  ))  or
(regexpr("([\w\-\(\)\.\s]+(?:\/[\w\-\(\)\.\s]+)+)",middle) = grantid and (fundingclass1="RIF" or fundingclass1="RPF") and regexprmatches("cyprus|rpf|\brif\b|research promotion foundation|research innovation foundation",lower(j2s(prev,middle,next))) ) or
(regexpr("(\d{3}\-\d{7}\-\d{4})",middle) = grantid and fundingclass1="MZOS" and regexprmatches("croatia|\bmses\b|\bmzos\b|ministry of science",lower(j2s(prev,middle,next))) ) or 
(regexpr("(\d{4})",middle) = grantid and fundingclass1="HRZZ" and (regexprmatches(normalizedacro,j2s(prev,middle,next)) or regexprmatches("croatian science foundation|\bhrzz\b",lower(j2s(prev,middle,next)) )     ) )

or  (fundingclass1="NWO" and regexpr("(\b(?:(?:(?:\w{2,3})(?:\.|\-)(?:\w{2,3})(?:\.|\-)(?:\w{2,3}))|(?:\d+))\b)",regexpr("\-",middle,"."))=nwo_opt1 and ((
 (regexprmatches("\bvici\b|\bvidi\b|dutch research council|\bveni\b|\bnwo\b|dutch|netherlands|\b"||lower(nwo_opt2)||"\b",lower(j2s(prev,middle,next)))) and not regexprmatches("^\d+$",nwo_opt1))
 or (regexprmatches("\bnwo\b|dutch research council",lower(j2s(prev,middle,next)))) ))

                                                                                                                         
or  (fundingclass1="SNSF" and regexpr('0{0,1}(\d{5,6})',middle)=grantid and regexprmatches('(?:\bsnsf\b)|(?:swiss national (?:science)?\s?foundation\b)',lower(j2s(prev,middle,next)))
)

 group by docid,id

)
)

union all

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingclass1, grantid from (

select docid,id,fundingclass1, grantid,prev,middle,next from (select * from (setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(regexpr("\n",c2," "),7,2,3,"\w{1,3}\s*\d{1,5}(?:(?:\-\w\d{2})|\b)") from (setschema 'c1,c2' select * from pubs where c2 is not null) ) ,grants where fundingclass1="FWF" and regexpr("(\w{1,3}\s*\d{1,5})",middle) = grantid and (regexprmatches("austrian|fwf",lower(j2s(prev,middle,next))) or regexprmatches(alias,j2s(prev,middle,next))  )) group by docid,id
)


union all 

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5), 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingclass1, grantid from ( select prev,middle,next,docid,id,max(confidence) as confidence, small_string, string, fundingclass1, grantid from ( select docid, id,
    (fullprojectmatch*10
    +coreprojectmatch*10
    +(activitymatch>0)*(administmatch>0)*length(nih_serialnumber)*2.5
    +(activitymatch>0)*length(nih_serialnumber)*0.666
    +(administmatch>0)*length(nih_serialnumber)*1
    +orgnamematch+nihposshortmatch*2+nihposfullmatch*5
    +nihpositivematch-nihnegativematch)*0.0571
    as confidence, nih_serialnumber, small_string, string, fundingclass1, grantid,string as prev,"" as middle,"" as next
    from (
      unindexed select regexpcountuniquematches('(?:[\W\d])'||nih_activity||'(?=[\W\w])(?!/)', small_string) as activitymatch,
          regexpcountuniquematches('(?:[\WA-KIR\d])'||nih_administeringic||'(?=[\W\d])(?!/)', small_string) as administmatch,
          regexpcountwords('\b(?i)'||keywords(nih_orgname)||'\b', keywords(string)) as orgnamematch,
          regexprmatches(grantid, small_string) as fullprojectmatch,
          regexprmatches(nih_coreprojectnum, small_string) as coreprojectmatch,
          regexpcountuniquematches(var('nihposshort'), string) as nihposshortmatch,
          regexpcountuniquematches(var('nihposfull'), string) as nihposfullmatch,
          regexpcountuniquematches(var('nihpositives'), string) as nihpositivematch,
          regexpcountuniquematches(var('nihnegatives'), string) as nihnegativematch,
          docid, id, nih_serialnumber, length(nih_serialnumber) as serialnumberlength, small_string, string, fundingclass1, grantid
          from (
           select docid, middle, j2s(prev1, prev2, prev3, prev4, prev5, prev6, prev7, prev8, prev9, prev10, middle, next1, next2, next3, next4, next5) as string, j2s(prev9, prev10, middle) as small_string
              from ( setschema 'docid, prev1, prev2, prev3, prev4, prev5, prev6, prev7, prev8, prev9, prev10, middle, next1, next2, next3, next4, next5'
              select c1 as docid, textwindow(regexpr('\n',c2,''),10,5,1,'\d{4,7}\b') from pubs where c2 is not null
            )), grants
            WHERE fundingclass1='NIH' and regexpr('^0+(?!\.)',regexpr('(\d{3,})',middle),'') = nih_serialnumber AND (activitymatch OR administmatch)
    ) where confidence > 0.5) group by docid,nih_serialnumber)

union all 

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5), 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1, docid, id, fundingClass1, grantid from ( select prev,middle,next,docid,id,max(confidence) as confidence, fundingClass1, grantid from ( select docid, id, fundingClass1, grantid,prevpack as prev,middle,nextpack as next,
      case when fundingClass1="WT" then /*wellcome trust confidence*/
                (regexpcountwords(var('wtpospos'),j2s(prevpack,nextpack)) * regexpcountwords('(?:collaborative|joint call)',j2s(prevpack,nextpack)))*0.33 +
                regexprmatches('\d{5}ma(?:\b|_)',middle)+
                regexprmatches('(?:\d{5}_)(?:z|c|b|a)(?:_\d{2}_)(?:z|c|b|a)',middle)*2+
                regexpcountwords(var('wtpospos'),prev)*0.5+
                regexprmatches(var('wtpospos'),middle)+
                regexpcountwithpositions(var('wtpospos'),prevpack)*0.39 +
                0.21*regexpcountwithpositions(var('wtpospos'),nextpack,1) -
                (regexprmatches(var('wtnegheavy'),middle) + regexprmatches('(?:n01|r01|dms)',middle) +regexprmatches('(?:ns|mh|hl|hd|ai)(?:_|)\d{5}',middle))*10 -
                4*regexpcountwords('(?:a|g|c|t|u){4}',middle) -
                regexprmatches(var('wtneglight'),middle)*0.3 -
                regexpcountwithpositions(var('wtnegheavy'),prevpacksmall,0,1,0.5)*0.39 -
                regexpcountwithpositions(var('wtneglight'),prevpacksmall,0,1,0.5)*0.18 -
                0.45*regexpcountwithpositions(var('wtneglight'),nextpack) -
                0.21*regexpcountwithpositions(var('wtnegheavy'),nextpack,1) 
       when fundingClass1="NSF" then
            regexpcountwords("\bnsf\b|national science foundation",j2s(prevpack,middle,nextpack)) - 
            5 * regexpcountwords("china|shanghai|danish|nsfc|\bsnf\b|bulgarian|\bbnsf\b|norwegian|rustaveli|israel|\biran\b|shota|georgia|functionalization|manufacturing",j2s(prevpack,middle,nextpack))
       when fundingClass1="RCN" and
            regexprmatches("norment|norway|\bnorweg|\brcn\b", j2s(prevpack,middle,nextpack)) 
            and regexprmatches("research|\bproject|\bgrant|\bcontract\b|acknowledge|funded|funding|\bfund\b|www rcn org", j2s(prevpack,middle,nextpack)) 
            and not (regexprmatches("europe|fp7|\berc\b|\bh2020\b",j2s(prevpack,middle)) and not regexprmatches("norment|norway|\bnorweg|\brcn\b",j2s(prevpack,middle))) 
            and (regexpcountwithpositions("europe|fp7|\berc\b|\bh2020\b",j2s(prevpack,middle)) - regexpcountwithpositions("norment|norway|\bnorweg|\brcn\b",j2s(prevpack,middle))) <= 0
            and not regexprmatches("fp7 funded project rcn", j2s(prevpack,middle,nextpack))
            and not regexprmatches("tel_|fax_", prevpacksmall)
            then 1 
       when fundingClass1="MESTD" then
            regexpcountwords("serbia|mestd|451_03_68",j2s(prevpacksmall,middle,nextpack))
       when fundingClass1="SFRS" then
            regexpcountwords('promis|\bsfrs\b|sciencefundrs|(?:\b|_|\d)'||normalizedacro||'(?:\b|_|\d)',j2s(prevpacksmall,middle,nextpack))
       when fundingClass1="GSRI" then
            regexpcountwords("gsrt|\bgsri\b",j2s(prevpacksmall,middle,nextpack))
       when fundingClass1="EC"/* fp7 confidence */ then
            case when fundingClass2 = "FP7" THEN
            regexprmatches(var('fp7middlepos'),middle)+
                regexprmatches('(?:\b|_|\d)'||normalizedacro||'(?:\b|_|\d)',j2s(middle,prevpacksmall,nextpack))*2  +
                regexprmatches('fp7',prev15)*0.4 +
                0.4*regexpcountwithpositions(var('fp7pospos'),prevpacksmall) +
                0.16*regexpcountwords(var('fp7pos'),prevpacksmall) +
                0.1*regexpcountwithpositions(var('fp7pospos'),nextpack,1) +
                regexpcountwords(var('fp7pos'),nextpack)*0.04 -
                regexprmatches(var('fp7negheavy'),middle)*1 -
                0.4*regexpcountwords('(a|g|c|t|u){4}',middle) -
                regexprmatches(var('fp7neglight'),middle)*0.3 -
                regexpcountwithpositions(var('fp7negheavy'),prevpacksmall)*0.48 -
                regexpcountwithpositions(var('fp7neglight'),prevpacksmall)*0.18 -
                (((regexpcountwords(('\b_*\d+_*\b'),prevpacksmall)+ (regexpcountwords(('\b_*\d+_*\b'),nextpack)))/4))*0.2 -
                regexpcountwithpositions(var('fp7neglight'),nextpack)*0.03 -
                regexpcountwithpositions(var('fp7negheavy'),nextpack,1)*0.08
                when fundingClass2="H2020" then
                    2*regexprmatches(normalizedacro,prevpack||" "||middle||" "||nextpack) +
                    regexpcountwords("h2020|horizon\s*2020|european\s*research\s*council|\berc\b|sk\wodowska|curie grant|marie\s*sklodowska\s*curie|marie\s*curie|european\s*commission|\beu\s*grant|\beu\s*project|\bec\s*grant|\bec\s*project|european\s*union",j2s(prevpack,middle,nextpack))
                when fundingClass2="HE" then
                    2*regexprmatches(normalizedacro,prevpack||" "||middle||" "||nextpack) +
                     regexpcountwords("horizon europe|\bhorizon\b|european\s*research\s*council|\berc\b|sk\wodowska|curie grant|marie\s*sklodowska\s*curie|marie\s*curie|european\s*commission|\beu\s*grant|\beu\s*project|\bec\s*grant|\bec\s*project|european\s*union", j2s(prevpack,middle,nextpack))
            end
       end as confidence
                from
                ( select id,fundingClass1,fundingClass2,docid,normalizedacro, j2s(prev14,prev15) as prev,grantid,prev15,j2s(prev1, prev2, prev3, prev4, prev5,prev6,prev7,prev8,prev9,prev10,prev11,prev12,prev13,prev14,prev15) as prevpack ,j2s(prev9,prev10,prev11,prev12,prev13,prev14,prev15) as prevpacksmall , middle, j2s(next1, next2, next3) as nextpack
                    from
                        ( 
                         select * from (setschema 'docid,prev1, prev2, prev3, prev4, prev5, prev6, prev7, prev8, prev9, prev10, prev11, prev12, prev13, prev14, prev15, middle, next1, next2, next3' select  c1 as docid ,textwindow(regexpr('(\b\S*?[^0-9\s_]\S*?\s_?)(\d{3})(\s)(\d{3})(_?\s\S*?[^0-9\s_]\S*?\b)',filterstopwords(normalizetext(lower(c2))),'\1\2\4\5'),15,3,'((?:(?:\b|\D)0|_|\b|\D)(?:\d{5}))|(((\D|\b)\d{6}(\D|\b)))|(?:(?:\D|\b)(?:\d{7})(?:\D|\b))|(?:(?:\D|\b)(?:\d{9})(?:\D|\b)) ' )
                            from   (setschema 'c1,c2' select * from  pubs where c2 is not null)) ,grants
                            where  (not regexprmatches( '(?:0|\D|\b)+(?:\d{8,})',middle) and not regexprmatches('(?:\D|\b)(?:\d{7})(?:\D|\b)',middle) and regexpr('(?:0|\D|\b)+(\d{5})',middle) = grantid and fundingclass1  in ('WT', 'EC') ) or ((not regexprmatches('(\d{6,}(?:\d|i\d{3}_?\b))|(jana\d{6,})', middle)) and not regexprmatches('(?:\D|\b)(?:\d{7})(?:\D|\b)',middle) 
                        and regexpr('(\d{6})',middle) = grantid and fundingclass1 in ('WT', 'EC','RCN')) or (regexprmatches('(?:(?:\D|\b)(?:\d{7})(?:\D|\b))',middle) and regexpr("(\d{7})",middle) = grantid and fundingclass1='NSF' ) 
                        or ( regexpr("(\d{5,6})",middle) = grantid and fundingclass1='MESTD' ) or (regexpr("(\d{6})",middle) = grantid and fundingclass1='GSRI') 
                        or (regexpr("(\d{7})",middle) = grantid and fundingclass1='SFRS') or (regexpr('(\d{9})',middle) = grantid and fundingclass2 in ('H2020', 'HE'))
                        )
                      ) where confidence > 0.16) group by docid,id);

delete from output_table where fundingClass1="CHIST-ERA" and grantid="unidentified" and docid in (select docid from output_table where grantid!="unidentified");
delete from matched_undefined_miur_only where docid in (select docid from output_table where fundingClass1="MIUR");
delete from matched_undefined_wt_only where docid in (select docid from output_table where fundingClass1="WT");
delete from matched_undefined_gsri where docid in (select docid from output_table where fundingClass1="GSRI");
delete from hfri_unidentified_only where docid in (select docid from output_hfri);

delete from output_table where j2s(docid,id) in (select j2s(T.docid, T.id) from output_table S, output_table T where  S.docid = T.docid and S.id in (select id from grants where grantid in (select * from gold)) and T.id in (select id from grants where grantid in ("246686", "283595","643410")));
delete from output_table where fundingclass1 = "EC" and j2s(docid, grantid) in (select j2s(docid, grantid) from output_table where fundingclass1 = "RCN");
create temp table secondary_output_table as 
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5), 'textsnippet', context) as C1, docid, id, fundingclass1, grantid, context from ( select docid,id,confidence, docid, id,  fundingclass1, grantid, context from ( select
0.8 as confidence, docid, id, fundingclass1, grantid, context
from (
unindexed select docid, regexpr("(\d+)",middle) as middle, comprspaces(j2s(prev1,prev2,prev3,prev4,prev5,prev6,prev7,prev8,prev9,prev10,prev11,prev12,prev13,"<<<",middle,">>>",next)) as context
from (
  setschema 'docid,prev1,prev2,prev3,prev4,prev5,prev6,prev7,prev8,prev9,prev10,prev11,prev12,prev13,middle,next' select c1, textwindow(lower(c2),-13,0,1, '\b\d{5,6}\b') from pubs where c2 is not null
) where CAST(regexpr("(\d+)",middle) AS INT)>70000), grants
WHERE fundingclass1="AKA" and (regexprmatches("[\b\s]academy of finland[\b\s]", context) or regexprmatches("[\b\s]finnish (?:(?:programme for )?cent(?:re|er)s? of excellence|national research council|funding agency|research program)[\b\s]", context) or regexprmatches("[\b\s]finnish academy[\b\s]", context)) and grantid=middle
) group by docid,id);

delete from secondary_output_table where docid||grantid in (select docid||grantid from output_table where fundingclass1="EC") and (regexpcountwithpositions('[\b\s]fp7[\b\s-]', context) > regexpcountwithpositions('(?:[\b\s]academy of finland[\b\s])|(?:[\b\s]finnish (?:(?:programme for )?cent(?:re|er)s? of excellence|national research council|funding agency|research program)[\b\s])|(?:[\b\s]finnish academy[\b\s])', context));


select C1 from output_table
union all
select C1 from secondary_output_table
union all
select C1 from output_hfri
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', prev||" "||middle||" "||next) from matched_undefined_miur_only
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', prev||" "||middle||" "||next) from matched_undefined_wt_only
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', prev||" "||middle||" "||next) from matched_undefined_inca_only
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', prev||" "||middle||" "||next) from hrb_unidentified_only
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', prev||" "||middle||" "||next) from matched_undefined_gsri
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', prev||" "||middle||" "||next) from matched_undefined_irc_only
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', prev||" << "||middle||" >> "||next) from (select * from hfri_unidentified_only group by docid);
