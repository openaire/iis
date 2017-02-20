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
hidden var 'wt_unidentified' from select 1 where (select count(*) from (select id from grants where id="40|wt__________::1e5e62235d094afd01cd56e65112fc63" limit 1)) > 0;


create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();

create temp table matched_undefined_wt_only as select distinct docid, "40|wt__________::1e5e62235d094afd01cd56e65112fc63" as id from (setschema 'docid,prev,middle,next'
select c1 as docid, textwindow2s(c2,20,2,3, '(\bWel?lcome Trust\b|\bWT\b)') from (setschema 'c1,c2' select * from pubs where c2 is not null)) where var('wt_unidentified')=1 and (regexprmatches('\bWel?lcome Trust\b', middle) or 
regexpcountwords('(?:\bwell?come trust\b)|(?:(?:\bthis work was|financial(?:ly)?|partial(?:ly)?|partly|(?:gratefully\s)?acknowledges?)?\s?\b(?:support|fund|suppli?)(?:ed|ing)?\s(?:by|from|in part\s(?:by|from)|through)?\s?(?:a)?\s?(?:grant)?)|(?:(?:programme|project) grant)|(?:(?:under|through)?\s?(?:the)?\s(?:grants?|contract(?:\snumber)?)\b)|(?:\bprograms? of\b)|(?:\bgrants? of\b)|(?:\bin part by\b)|(?:\bthis work could not have been completed without\b)|(?:\bcontract\b)|(?:\backnowledgments?\b)', lower(prev||' '||middle||' '||next)) > 3);

create temp table output_table as 

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5)) as C1, docid, id, fundingclass1 from ( select docid,id,max(confidence) as confidence, docid, id,  fundingclass1 from ( select 
(0.3
+hardmatch*0.6
+subrcukmatch
+rcukmatch)
as confidence, docid, id, fundingclass1
from (
unindexed select regexprmatches('\bRCUK\b|[Rr]esearch [Cc]ouncils UK', context) as rcukmatch,
regexprmatches('\b'||rcuk_subfunder||'\b', context) as subrcukmatch,
regexprmatches('(?:G\d{6,7})|(?:[A-Z]{2}\/\w{6,7}\/\d{1,2}(?:\/xx)?)|(?:(?:BBS|PPA)\/[A-Z](?:\/[A-Z])?\/(?:\w{8,9}|(?:\d{4}\/)?\d{5}(?:\/\d)?))|(?:(?:RES|PTA)-\d{3}-\d{2}-\d{4}(?:-[A-Z]|-\d{1,2})?)|(?:MC_(?:\w{8,10}|\w{2}_(?:\w{2,4}_)?(?:\d{4,5}|[UG]\d{7,9})(?:\/\d{1,2})?))|(?:MC_\w{2}_\w{2}(?:_\w{2})?\/\w{7}(?:\/\d)?)|(?:ESPA-[A-Z]{3,6}-\d{4}(?:-[A-Z]{3}-\d{3}|-\d{3})?)', middle) as hardmatch,
docid, id, fundingclass1
from (
select docid, stripchars(middle,'.)(,[]') as middle, prev, next, prev||' '||middle||' '||next as context
from (
  setschema 'docid,prev,middle,next' select c1, textwindow2s(c2,15,1,1,'(?:[SG]?\d{5,7}(?:\/\d)?)|(?:[A-Z]{2}\/\w{6,7}\/\d{1,2}(?:\/xx)?)|(?:[GE]\d{2,3}\/\d{1,4})|(?:(?:BBS|PPA)\/[A-Z](?:\/[A-Z])?\/(?:\w{8,9}|(?:\d{4}\/)?\d{5}(?:\/\d)?))|(?:(?:RES|PTA)-\d{3}-\d{2}-\d{4}(?:-[A-Z]|-\d{1,2})?)|(?:MC_(?:\w{8,10}|\w{2}_(?:\w{2,4}_)?(?:\d{4,5}|[UG]\d{7,9})(?:\/\d{1,2})?))|(?:MC_\w{2}_\w{2}(?:_\w{2})?\/\w{7}(?:\/\d)?)|(?:[A-Za-z]{3,9}\d{5,7}a?)|(?:ESPA-[A-Z]{3,6}-\d{4}(?:-[A-Z]{3}-\d{3}|-\d{3})?)') from pubs where c2 is not null
)), grants
WHERE middle = grantid
)  ) group by docid,id)

union all

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8) as C1, docid, id, fundingclass1 from (
select docid,id,fundingclass1 from (select docid,upper(regexpr("(\w+.*\d+)",middle)) as match,id,grantid,middle,fundingclass1  from (setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(c2,12,1,5,"(.+\/\w+\/\d{4}\W*\Z)|(\d{4})|(\d{6,7})|(\w{2}\d{4,})|(\w*\/[\w,\.]*\/\w*)|(?:\d{3}\-\d{7}\-\d{4})|(?:(?:\b|U)IP\-2013\-11\-\d{4}\b)|(\b(?:(?:(?:\w{2,3})(?:\.|\-)(?:\w{2,3})(?:\.|\-)(?:\w{2,3}))|(?:\d+))\b)") from (setschema 'c1,c2' select * from pubs where c2 is not null) ) , grants where 
(match = grantid and (fundingclass1 in ("FCT","ARC"))) or 
(regexpr("(\d{5,7})",middle)=grantid and fundingclass1 = "NHMRC" and regexprmatches("nhmrc|medical research|national health medical",filterstopwords(normalizetext(lower(j2s(prev,middle,next)))))) or 
(regexpr("(\w*\/[\w,\.]*\/\w*)",middle)=grantid and fundingclass1 = "SFI") or 
(regexpr("(\d{3}\-\d{7}\-\d{4})",middle) = grantid and fundingclass1="MZOS" and regexprmatches("croatia|\bmses\b|\bmzos\b|ministry of science",lower(j2s(prev,middle,next))) ) or 
(regexpr("(\d{4})",middle) = grantid and fundingclass1="HRZZ" and (regexprmatches(normalizedacro,j2s(prev,middle,next)) or regexprmatches("croatian science foundation|\bhrzz\b",lower(j2s(prev,middle,next)) )     ) 
or  (fundingclass1="NWO" and regexpr("(\b(?:(?:(?:\w{2,3})(?:\.|\-)(?:\w{2,3})(?:\.|\-)(?:\w{2,3}))|(?:\d+))\b)",middle)=nwo_opt1 and 
regexprmatches("\bvici\b|\bvidi\b|\bveni\b|\bnwo\b|dutch|netherlands|\b"||lower(nwo_opt2)||"\b",lower(j2s(prev,middle,next))))
or  (fundingclass1="SNSF" and regexpr('0{0,1}(\d{5,6})',middle)=grantid and regexprmatches('(?:\bsnsf\b)|(?:swiss national (?:science)?\s?foundation\b)',lower(j2s(prev,middle,next)))
)

) group by docid,id

)
)

union all

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8) as C1, docid, id, fundingclass1 from (

select docid,id,fundingclass1 from (select * from (setschema 'docid,prev,middle,next' select c1 as docid,textwindow2s(regexpr("\n",c2," "),7,2,3,"\w{1,3}\s*\d{1,5}(?:(?:\-\w\d{2})|\b)") from (setschema 'c1,c2' select * from pubs where c2 is not null) ) ,grants where regexpr("(\w{1,3}\s*\d{1,5})",middle) = grantid and (regexprmatches("austrian|fwf",lower(j2s(prev,middle,next))) or regexprmatches(alias,j2s(prev,middle,next))  )) group by docid,id
)


union all 

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5)) as C1, docid, id, fundingclass1 from ( select docid,id,max(confidence) as confidence, small_string, string, fundingclass1 from ( select docid, id,
    (fullprojectmatch*10
    +coreprojectmatch*10
    +(activitymatch>0)*(administmatch>0)*length(nih_serialnumber)*2.5
    +(activitymatch>0)*length(nih_serialnumber)*0.666
    +(administmatch>0)*length(nih_serialnumber)*1
    +orgnamematch+nihposshortmatch*2+nihposfullmatch*5
    +nihpositivematch-nihnegativematch)*0.0571
    as confidence, nih_serialnumber, small_string, string, fundingclass1
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
          docid, id, nih_serialnumber, length(nih_serialnumber) as serialnumberlength, small_string, string, fundingclass1
          from (
           select docid, middle, j2s(prev1, prev2, prev3, prev4, prev5, prev6, prev7, prev8, prev9, prev10, middle, next1, next2, next3, next4, next5) as string, j2s(prev9, prev10, middle) as small_string
              from ( setschema 'docid, prev1, prev2, prev3, prev4, prev5, prev6, prev7, prev8, prev9, prev10, middle, next1, next2, next3, next4, next5'
              select c1 as docid, textwindow(regexpr('\n',c2,''),10,5,1,'\d{4,7}\b') from pubs where c2 is not null
            )), grants
            WHERE regexpr('^0+(?!\.)',regexpr('(\d{3,})',middle),'') = nih_serialnumber AND (activitymatch OR administmatch)
    ) where confidence > 0.5) group by docid,nih_serialnumber)

union all 

select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', sqroot(min(1.49,confidence)/1.5)) as C1, docid, id, fundingClass1 from ( select docid,id,max(confidence) as confidence, fundingClass1 from ( select docid, id, fundingClass1,
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
                    regexpcountwords("h2020|horizon 2020|european research council|\berc\b",j2s(prevpack,middle,nextpack))
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
                        or ( regexpr("(\d{5,6})",middle) = grantid and fundingclass1='MESTD' )
                        )
                      ) where confidence > 0.16) group by docid,id);

delete from matched_undefined_wt_only where docid in (select docid from output_table where fundingClass1="WT");

select C1 from output_table
union all
select jdict('documentId', docid, 'projectId', id, 'confidenceLevel', 0.8) from matched_undefined_wt_only;