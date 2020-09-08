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
hidden var 'miur_unidentified' from select id from grants where fundingclass1="MIUR" and grantid="unidentified" limit 1;
hidden var 'wt_unidentified' from select id from grants where fundingclass1="WT" and grantid="unidentified" limit 1;


create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();

create temp table matched_undefined_miur_only as select distinct docid, var('miur_unidentified') as id from (setschema 'docid,prev,middle,next'
select c1 as docid, textwindow2s(c2,10,1,10, '\b(?:RBSI\d{2}\w{4})\b') from (setschema 'c1,c2' select * from pubs where c2 is not null)) where var('miur_unidentified') and (regexprmatches('\b(?:RBSI\d{2}\w{4})\b', middle));

create temp table matched_undefined_wt_only as select distinct docid, var('wt_unidentified') as id from (setschema 'docid,prev,middle,next'
select c1 as docid, textwindow2s(c2,20,2,3, '(\bWel?lcome Trust\b|\bWT\b)') from (setschema 'c1,c2' select * from pubs where c2 is not null)) where var('wt_unidentified') and (regexprmatches('\bWel?lcome Trust\b', middle) or 
regexpcountwords('(?:\bwell?come trust\b)|(?:(?:\bthis work was|financial(?:ly)?|partial(?:ly)?|partly|(?:gratefully\s)?acknowledges?)?\s?\b(?:support|fund|suppli?)(?:ed|ing)?\s(?:by|from|in part\s(?:by|from)|through)?\s?(?:a)?\s?(?:grant)?)|(?:(?:programme|project) grant)|(?:(?:under|through)?\s?(?:the)?\s(?:grants?|contract(?:\snumber)?)\b)|(?:\bprograms? of\b)|(?:\bgrants? of\b)|(?:\bin part by\b)|(?:\bthis work could not have been completed without\b)|(?:\bcontract\b)|(?:\backnowledgments?\b)', lower(prev||' '||middle||' '||next)) > 3);

create temp table output_table as

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8,'textsnippet',j2s(prev,middle,next)) as C1, docid, id, fundingclass1, grantid from (
select docid,id,fundingclass1, grantid,prev,middle,next from (select * from (setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(regexpr("\n",c2," "),10,1,10, '(?:RBSI\d{2}\w{4})|(?:2015\w{6})') from (setschema 'c1,c2' select * from pubs where c2 is not null) ) ,grants where fundingclass1="MIUR" and regexpr("((?:RBSI\d{2}\w{4})|(?:2015\w{6}))",middle) = grantid)
group by docid,id)

union all

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5),'textsnippet',j2s(prev,middle,next)) as C1, docid, id, fundingclass1, grantid from 
( select prev,middle,next,docid,id,max(confidence) as confidence, docid, id,  fundingclass1, grantid from ( select 
(0.3
+hardmatch*0.6
+subrcukmatch
+rcukmatch)
as confidence, docid, id,prev,middle,next, fundingclass1, grantid
from (
unindexed select regexprmatches('\bRCUK\b|[Rr]esearch [Cc]ouncils UK', context) as rcukmatch,
regexprmatches('\b'||rcuk_subfunder||'\b', context) as subrcukmatch,
regexprmatches('(?:G\d{6,7})|(?:[A-Z]{2}\/\w{6,7}\/\d{1,2}(?:\/xx)?)|(?:(?:BBS|PPA)\/[A-Z](?:\/[A-Z])?\/(?:\w{8,9}|(?:\d{4}\/)?\d{5}(?:\/\d)?))|(?:(?:RES|PTA)-\d{3}-\d{2}-\d{4}(?:-[A-Z]|-\d{1,2})?)|(?:MC_(?:\w{8,10}|\w{2}_(?:\w{2,4}_)?(?:\d{4,5}|[UG]\d{7,9})(?:\/\d{1,2})?))|(?:MC_\w{2}_\w{2}(?:_\w{2})?\/\w{7}(?:\/\d)?)|(?:ESPA-[A-Z]{3,6}-\d{4}(?:-[A-Z]{3}-\d{3}|-\d{3})?)', middle) as hardmatch,
docid, id, fundingclass1, grantid,middle,prev,next
from (
select docid, stripchars(middle,'.)(,[]') as middle, prev, next, prev||' '||middle||' '||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(c2,15,1,1,'(?:[SG]?\d{5,7}(?:\/\d)?)|(?:[A-Z]{2}\/\w{6,7}\/\d{1,2}(?:\/xx)?)|(?:[GE]\d{2,3}\/\d{1,4})|(?:(?:BBS|PPA)\/[A-Z](?:\/[A-Z])?\/(?:\w{8,9}|(?:\d{4}\/)?\d{5}(?:\/\d)?))|(?:(?:RES|PTA)-\d{3}-\d{2}-\d{4}(?:-[A-Z]|-\d{1,2})?)|(?:MC_(?:\w{8,10}|\w{2}_(?:\w{2,4}_)?(?:\d{4,5}|[UG]\d{7,9})(?:\/\d{1,2})?))|(?:MC_\w{2}_\w{2}(?:_\w{2})?\/\w{7}(?:\/\d)?)|(?:[A-Za-z]{3,9}\d{5,7}a?)|(?:ESPA-[A-Z]{3,6}-\d{4}(?:-[A-Z]{3}-\d{3}|-\d{3})?)') from pubs where c2 is not null
)), grants
WHERE fundingclass1="RCUK" and middle = grantid
) where confidence>0.3 ) group by docid,id)

union all
--DFG
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8,'textsnippet',j2s(prev,middle,next)) as C1, docid, id, fundingclass1, grantid from  
(setschema 'docid,prev,middle,next' select c1, textwindow2s(regexpr("\n",filterstopwords(keywords(c2)),"\s"),10,2,7,"\w{3}\s\d{1,4}" ) from pubs where c2 is not null), grants 
where lower(regexpr("\b(\w{3}\s\d{1,4})\b",middle)) = grantid and 
regexprmatches("support|project|grant|fund|thanks|agreement|research|acknowledge|centre|center|nstitution|program|priority|dfg|german|dutch|deutche",lower(j2s(prev,middle,next))) group by docid, id
--DFG

union all
-- Canadian funders
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8, 'textsnippet', textsnippet) as C1, docid, id, fundingclass1, grantid
from (
    select docid, case when regexprmatches(".*(?:(?:CIHR|IRSC)|(?i)(?:canad(?:ian|a) institute(?:s)? health research|institut(?:(?:e)?(?:s)?)? recherche sant(?:é|e) canada)).*", prev||" "||middle||" "||next) then (select id from grants where fundingclass1 = 'CIHR')
                       when regexprmatches(".*(?:(?:NSERC|CRSNG)|(?i)(?:nat(?:ural|ional) science(?:s)?(?:\sengineering(?:\sresearch)?|\sresearch) co(?:u)?n(?:c|se)(?:i)?l|conseil(?:s)? recherche(?:s)? science(?:s)? naturel(?:les)?(?:\sg(?:e|é)nie)? canada)).*", prev||" "||middle||" "||next) then (select id from grants where fundingclass1 = 'NSERC')
                       when regexprmatches(".*(?:(?:SSHRC|CRSH|SSRCC)|(?i)(?:social science(?:s)?|conseil(?:s)? recherche(?:s)?(?:\ssciences humaines)? canada|humanities\sresearch)).*", prev||" "||middle||" "||next) then (select id from grants where fundingclass1 = 'SSHRC')
                       else 'canadian_unspecified_id'
                  end as id, "unidentified" as grantid, "Canadian" as fundingclass1, (prev||" <<< "||middle||" >>> "||next) as textsnippet
    from
        (setschema 'docid,prev,middle,next' select c1, textwindow2s(filterstopwords(keywords(c2)), 15,1,15, "^(?:(?:(?:CIHR|IRSC)|(?:NSERC|CRSNG)|(?:SSHRC|CRSH))|(?i)(?:co(?:(?:un(?:cil|sel))|(?:nseil(?:s)?))|canad(?:a|ian)))$") from pubs where c2 is not null)
    where
    ( /* Terms */
        /* Acronyms */
        regexprmatches("^(?:CIHR|(?:NSERC|CRSNG)|(?:SSHRC|CRSH|SSRCC))$", middle)
        or (
            regexprmatches("^IRSC$", middle) /* This is the french acronym of CIHR. It also refers to some other organizations, so we search and exclude them. */
            and not regexprmatches(".*(?:informal relationships social capital|interlocus sexual conflict|international (?:rosaceae|rosbreed) snp consortium|iranian seismological cent(?:er|re)).*", lower(prev||" "||next))
        )
        or (/* Full-names */
            (   /* Middle: "Council", "Counsel", "Conseil", "Conseils" --> NSERC/CRSNG, SSHRC/CRSH/SSRCC */
                regexprmatches("^co(?:(?:un(?:cil|sel))|(?:nseil(?:s)?))$", lower(middle))
                and (
                    -- The "middle" at the beginning of the fullname.
                    (   regexprmatches("^recherche(?:s)?(?:(?:\s(?:g(?:e|é)nie|science(?:s)?)(?:\s(?:humaines|naturel(?:les)?)?)?(?:\sg(?:e|é)nie)?)?)?\scanada.*", lower(next))    -- The term "canada" is put as mandatory here, as we get false-positives.
                        or regexprmatches("^social\sscience(?:s)?\shumanities\sresearch\scanada.*", lower(next))  -- It has the word "canada", so it's a canadian match.
                    )
                    or -- The "middle" at the end of the fullname.
                    (
                        ( regexprmatches(".*(?:social|nat(?:ural|ional))\sscience(?:s)?\s(?:(?:engineering|humanities)(?:\sresearch)?|research)$", lower(prev))
                            or regexprmatches(".*humanities\sresearch$", lower(prev))
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
        regexprmatches(".*(?:fund|support|financ|proje(?:c)?t|grant|subvention|sponsor|parrain|souten|subsidiz|promot|acquir|acknowledg|administ|assist|donor|bailleur|g(?:e|é)n(?:e|é)rosit).*", lower(prev||" "||next))
        or regexprmatches(".*(?:thank|gratefull|(?:re)?merci).*", lower(prev))
    )
)
where id is not null
group by docid, id


union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8,'textsnippet',j2s(prev,middle,next)) as C1, docid, id, fundingclass1, grantid from (
select docid,id,fundingclass1,grantid,prev,middle,next from (select docid,upper(regexpr("(\w+.*\d+)",middle)) as match,id,grantid,middle,fundingclass1,grantid,prev,middle,next  from (
setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(c2,15,1,5,"(?:\bANR-\d{2}-\w{4}-\d{4}\b)|\b(?:06|07|10|11|12|13|14|15|16|17|18|19)\-\w{4}\-\d{4}(?:\-\d{2})*\b|(.+\/\w+\/\d{4}\W*\Z)|(\d{4})|(\d{2}-\d{2}-\d{5})|(\d{6,7})|(\w{2}\d{4,})|(\w+\/\w+\/\w+)|(\w*\/[\w,\.]*\/\w*)|(?:\d{3}\-\d{7}\-\d{4})|(?:(?:\b|U)IP\-2013\-11\-\d{4}\b)|(\b(?:(?:(?:\w{2,3})(?:\.|\-)(?:\w{2,3})(?:\.|\-)(?:\w{2,3}))|(?:\d+))\b)|(?:\b\d{7,8}\b)|(?:\b\d{3}[A-Z]\d{3}\b)|(?:[A-Z]{2,3}.+)|(?:\d{4}\-\w+\-\w+(\-\d+)*)|(?:\d{4}\-\d{2,})") from (setschema 'c1,c2' select * from pubs where c2 is not null) ) , grants where 
(match = grantid and (fundingclass1 in ("FCT","ARC"))) or 
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
(regexpr("(\w+\/\w+\/\w+)",middle) = grantid and fundingclass1="RPF" and regexprmatches("cyprus|rpf",lower(j2s(prev,middle,next))) ) or
(regexpr("(\d{3}\-\d{7}\-\d{4})",middle) = grantid and fundingclass1="MZOS" and regexprmatches("croatia|\bmses\b|\bmzos\b|ministry of science",lower(j2s(prev,middle,next))) ) or 
(regexpr("(\d{4})",middle) = grantid and fundingclass1="HRZZ" and (regexprmatches(normalizedacro,j2s(prev,middle,next)) or regexprmatches("croatian science foundation|\bhrzz\b",lower(j2s(prev,middle,next)) )     ) )
or  (fundingclass1="NWO" and regexpr("(\b(?:(?:(?:\w{2,3})(?:\.|\-)(?:\w{2,3})(?:\.|\-)(?:\w{2,3}))|(?:\d+))\b)",middle)=nwo_opt1 and 
regexprmatches("\bvici\b|\bvidi\b|\bveni\b|\bnwo\b|dutch|netherlands|\b"||lower(nwo_opt2)||"\b",lower(j2s(prev,middle,next))))
or  (fundingclass1="SNSF" and regexpr('0{0,1}(\d{5,6})',middle)=grantid and regexprmatches('(?:\bsnsf\b)|(?:swiss national (?:science)?\s?foundation\b)',lower(j2s(prev,middle,next)))
)

 group by docid,id

)
)

union all

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8,'textsnippet',j2s(prev,middle,next)) as C1, docid, id, fundingclass1, grantid from (

select docid,id,fundingclass1, grantid,prev,middle,next from (select * from (setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(regexpr("\n",c2," "),7,2,3,"\w{1,3}\s*\d{1,5}(?:(?:\-\w\d{2})|\b)") from (setschema 'c1,c2' select * from pubs where c2 is not null) ) ,grants where fundingclass1="FWF" and regexpr("(\w{1,3}\s*\d{1,5})",middle) = grantid and (regexprmatches("austrian|fwf",lower(j2s(prev,middle,next))) or regexprmatches(alias,j2s(prev,middle,next))  )) group by docid,id
)


union all 

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5),'textsnippet',j2s(prev,middle,next)) as C1, docid, id, fundingclass1, grantid from ( select prev,middle,next,docid,id,max(confidence) as confidence, small_string, string, fundingclass1, grantid from ( select docid, id,
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

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5), 'textsnippet',j2s(prev,middle,next)) as C1, docid, id, fundingClass1, grantid from ( select prev,middle,next,docid,id,max(confidence) as confidence, fundingClass1, grantid from ( select docid, id, fundingClass1, grantid,prevpack as prev,middle,nextpack as next,
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
       when fundingClass1="MESTD" then
            regexpcountwords("serbia|mestd",j2s(prevpacksmall,middle,nextpack))
       when fundingClass1="GSRT" then
            regexpcountwords("gsrt",j2s(prevpacksmall,middle,nextpack))
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
            end
       end as confidence
                from
                ( select id,fundingClass1,fundingClass2,docid,normalizedacro, j2s(prev14,prev15) as prev,grantid,prev15,j2s(prev1, prev2, prev3, prev4, prev5,prev6,prev7,prev8,prev9,prev10,prev11,prev12,prev13,prev14,prev15) as prevpack ,j2s(prev9,prev10,prev11,prev12,prev13,prev14,prev15) as prevpacksmall , middle, j2s(next1, next2, next3) as nextpack
                    from
                        ( 
                         select * from (setschema 'docid,prev1, prev2, prev3, prev4, prev5, prev6, prev7, prev8, prev9, prev10, prev11, prev12, prev13, prev14, prev15, middle, next1, next2, next3' select  c1 as docid ,textwindow(regexpr('(\b\S*?[^0-9\s_]\S*?\s_?)(\d{3})(\s)(\d{3})(_?\s\S*?[^0-9\s_]\S*?\b)',filterstopwords(normalizetext(lower(c2))),'\1\2\4\5'),15,3,'((?:(?:\b|\D)0|_|\b|\D)(?:\d{5}))|(((\D|\b)\d{6}(\D|\b)))|(?:(?:\D|\b)(?:\d{7})(?:\D|\b)) ' )
                            from   (setschema 'c1,c2' select * from  pubs where c2 is not null)) ,grants
                            where  (not regexprmatches( '(?:0|\D|\b)+(?:\d{8,})',middle) and not regexprmatches('(?:\D|\b)(?:\d{7})(?:\D|\b)',middle) and regexpr('(?:0|\D|\b)+(\d{5})',middle) = grantid and fundingclass1  in ('WT', 'EC') ) or ((not regexprmatches('(\d{6,}(?:\d|i\d{3}_?\b))|(jana\d{6,})', middle)) and not regexprmatches('(?:\D|\b)(?:\d{7})(?:\D|\b)',middle) 
                        and regexpr('(\d{6})',middle) = grantid and fundingclass1 in ('WT', 'EC')) or (regexprmatches('(?:(?:\D|\b)(?:\d{7})(?:\D|\b))',middle) and regexpr("(\d{7})",middle) = grantid and fundingclass1='NSF' ) 
                        or ( regexpr("(\d{5,6})",middle) = grantid and fundingclass1='MESTD' ) or (regexpr("(\d{6})",middle) = grantid and fundingclass1='GSRT')
                        )
                      ) where confidence > 0.16) group by docid,id);

delete from matched_undefined_miur_only where docid in (select docid from output_table where fundingClass1="MIUR");
delete from matched_undefined_wt_only where docid in (select docid from output_table where fundingClass1="WT");

delete from output_table where j2s(docid,id) in (select j2s(T.docid, T.id) from output_table S, output_table T where  S.docid = T.docid and S.id in (select id from grants where grantid in (select * from gold)) and T.id in (select id from grants where grantid in ("246686", "283595","643410")));

create temp table secondary_output_table as 
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5),'textsnippet',context) as C1, docid, id, fundingclass1, grantid, context from ( select docid,id,confidence, docid, id,  fundingclass1, grantid, context from ( select 
0.8 as confidence, docid, id, fundingclass1, grantid, context
from (
unindexed select docid, regexpr("(\d+)",middle) as middle, comprspaces(j2s(prev1,prev2,prev3,prev4,prev5,prev6,prev7,prev8,prev9,prev10,prev11,prev12,prev13,middle,next)) as context
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
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8,'textsnippet','') from matched_undefined_miur_only
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8,'textsnippet','') from matched_undefined_wt_only;
