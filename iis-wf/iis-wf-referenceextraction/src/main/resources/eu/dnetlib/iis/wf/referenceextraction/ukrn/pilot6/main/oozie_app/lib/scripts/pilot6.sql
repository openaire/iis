--For testing...
-- cd /storage/eleni/openAIRE/UKNR
-- attach database "/storage/eleni/openAIRE/06.Biomedical06/mydata.db" as d1;
-- create table mydata as select * from (setschema 'docid,text' select * from mydata where
--              docid ='PMC2931525' or  docid ='PMC2933899' or
--              docid ='PMC3737084' or  docid ='PMC3737070' );
-- output 'pubs.txt' select jdict('id', docid, 'text', text) from mydata;
--
--cp pubs.txt pubs.json
--cat pubs.json | python ~/Desktop/openAIRE/madis2/src/mexec.py -f pilot6_v4.sql -d test01.db > results_v4.json

--cat pubs_empty.json | python ~/Desktop/openAIRE/madis2/src/mexec.py -f pilot6.sql -d test01.db > results_v2.json


create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());

hidden var 'ASR' from "\b10\.17590\/asr\.\d+\b";
hidden var 'ASR1' from "asr\.\d+\b";
hidden var 'AsPredicted1' from "\bAsPredicted\s?#\d+\b|aspredicted\.org";
hidden var 'AsPredicted2' from "\bAsPredicted\s?#\d+\b";
hidden var 'AsPredicted3' from "aspredicted\.org\/blind\.php\?x ?=\/? ?[a-zA-Z0-9]{3}_?[a-zA-Z0-9]{3}|aspredicted\.org\/[a-z0-9]{5}\.pdf|aspredicted\.org\/[A-Z0-9]{3}_?[A-Z0-9]{3}";
hidden var 'ANZCTR1' from "\bACTRN:{0,1}\s{0,1}\d+p{0,1}\b|anzctr\.org\.au";
hidden var 'ANZCTR2' from "\bACTRN:{0,1}\s{0,1}\d+p{0,1}\b";
hidden var 'ANZCTR3' from "anzctr\.org\.au\/Trial\/Registration\/TrialReview\.aspx\?id= ?\d{6}";
hidden var 'ReBec' from "\bRBR-\d+[a-z0-9]+\b";

hidden var 'ChiCTR1' from "\bChiCTR\s{0,1}-{0,1}(?:TRC){0,1}-{0,1}\d+\b|chictr\.org\.cn";
hidden var 'ChiCTR2' from "\bChiCTR\s{0,1}-{0,1}(?:TRC){0,1}-{0,1}\d+\b";
hidden var 'ChiCTR3' from "chictr\.org\.cn\/showproj.html\?proj=\d+|chictr\.org\.cn\/showproj.aspx\?proj=\d+|chictr\.org\.cn\/showprojen.aspx\?proj=\d+|chictr\.org\.cn\/edit.aspx\?pid=\d+|chictr\.org\.cn\/hvshowproject\.aspx\?id=\d+|chictr\.org\.cn\/hvshowprojectEN.html\?id=\d+|chictr\.org\.cn\/showprojEN\.html\?proj=\d+|chictr\.org\.cn\/bin\/project\/edit\?pid=\d+";

hidden var 'CRiS1' from "cris\.nih\.go\.kr|\bKCT\s{0,1}\d{7}\b";
hidden var 'CRiS2' from "\bKCT\s{0,1}\d{7}\b";
hidden var 'CRiS3' from "cris\.nih\.go\.kr\/cris\/search\/detailSearch\.do\?seq=\d+";
-- den brhka kapoio apotelesma me url

hidden var 'CTIS' from "\bEU\s{0,1}CT\s{0,1}\d{4}-\d+-\d{2}-\d{2}\b|\bEU\s{0,1}CT\s{0,1}number\s{0,1}\d{4}-\d+-\d{2}-\d{2}\b";
-- den brhka kapoio apotelesma meto EUCT

hidden var 'CTRI1' from "ctri\.nic\.in|\bCTRI/\d{4}/\d{2}/\d+\b";
hidden var 'CTRI2' from "\bCTRI/\d{4}/\d{2}/\d+\b";
hidden var 'CTRI3' from "ctri\.nic\.in\/Clinicaltrials\/pmaindet2\.php\?trialid=\d+|ctri\.nic\.in\/Clinicaltrials\/pdf_generate\.php\?trialid=\d+|ctri\.nic\.in\/Clinicaltrials\/rmaindet\.php\?trialid=\d+|ctri\.nic\.in\/Clinicaltrials\/showallp\.php\?mid1=\d+";
hidden var 'CT_gov' from "NCT\s{0,1}\d{6,}";
hidden var 'RPCEC' from "\bRPCEC\s{0,1}\d+\b";
hidden var 'LTR' from "onderzoekmetmensen\.nl\/[a-z]{2}\/trial\/\d+|\bNL\s{0,1}\d+\b|\bNTR\s{0,1}\d+\b";
-- eixa lathos thn prohgoumenh fora
hidden var 'LTR_positivewords' from 'onderzoekmetmensen|dutch trial';
hidden var 'EU_CTR' from "\d{4}[-–]\d+-\d+";
hidden var 'EU_CTR_positivewords' from 'eudract|ctr-search';
hidden var 'DRKS' from "\bDRKS\s{0,1}\d+\b";
hidden var 'ICTRP' from "\bU\d{4}-\d{4}-\d{4}\b";
hidden var 'INPLASY' from "\inplasy\s{0,1}\d+\b|inplasy-\d{4}-\d{2}-\d+";
hidden var 'PROSPERO' from "www\.crd\.york\.ac\.uk\/prospero|\bCRD\s{0,1}\d{6,}\b";
hidden var 'PROSPERO1' from "www\.crd\.york\.ac\.uk\/prospero\/display_record\.php\?RecordID=\d+";
hidden var 'PROSPERO1b' from "www\.crd\.york\.ac\.uk\/prospero\/display_record\.php\?RecordID=(\d+)";
hidden var 'PROSPERO2' from "\bCRD\s{0,1}\d{6,}\b";

hidden var 'ISRCTN' from "\bISRCTN\s{0,1}\d+\b";
hidden var 'ITMCTR' from "\bITMCTR\s{0,1}\d+\b|ccebtcm\.org\.cn\/[a-z]{2}-[A-Z]{2}\/Home\/ProjectView\?pid=[a-z0-9-]+";
hidden var 'IRCT' from "\bIRCT\s{0,1}\d+N{0,1}\d+\b";
hidden var 'JMACCT' from "\bJMA-IIA\s{0,1}\d+\b";
hidden var 'JAPIC' from "\bJapicCTI\s{0,1}-{0,1}\d+\b";
hidden var 'jRCT' from "\bjRCTs{0,1}\s{0,1}\d+\b";
hidden var 'LBCTR' from "lbctr\.moph\.gov\.lb\/Trials\/Details\/\d+|\bLBCTR\s{0,1}\d+\b";
hidden var 'OSF' from "osf.io\/\s{0,1}\w{5}\b";
hidden var 'OSF1' from "osf.io\/\s{0,1}(\w{5})\b";
hidden var 'PACTR' from "pactr\.samrc\.ac\.za|\bPACTR\s{0,1}\d+\b";
hidden var 'PACTR2' from "\bPACTR\s{0,1}\d+\b";
hidden var 'PACTR3' from "\bpactr\.samrc\.ac\.za\/TrialDisplay\.aspx\?TrialID=\d+\b|\bpactr\.samrc\.ac\.za\/Researcher\/TrialRegister\.aspx\?TrialID=\d+\b";
hidden var 'REPEC' from "\brepec\b|\bREPEC\b";
hidden var 'PCT' from "\bPCTE\s{0,1}\d{3,}\b";
-- I changed from \d+ to \d{3,}

hidden var 'ResearchRegistry' from "www\.researchregistry\.com|\bresearchregistry\s{0,1}\d+\b";
hidden var 'ResearchRegistry1' from "www\.researchregistry\.com\/browse-the-registry#registryofsystematicreviewsmeta-analyses\/registryofsystematicreviewsmeta-analysesdetails\/[a-z0-9]+\/|www\.researchregistry\.com\/browse-the-registry#home\/registrationdetails\/[a-z0-9]+\/|www\.researchregistry\.com\/browse-the-registry#registryofsystematicreviewsmetaanalyses\/registryofsystematicreviewsmeta-analysesdetails\/[a-z0-9]+\/";
hidden var 'ResearchRegistry2' from "\bresearchregistry\s{0,1}\d+\b";
hidden var 'SLCTR' from "\bslctr/\d{4}/\d+\b";
hidden var 'TCTR' from "\bTCTR\s{0,1}\d+\b";
hidden var 'UMIN' from "\b(?:JPRN-)?UMIN\s{0,1}\d+\b";

select jdict('query', 'ASR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('ASR1')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ASR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'AsPredicted', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
        select docid, prev, middle, next,
                case when regexprmatches(var('AsPredicted2'), middle)
                     then regexpr("("||var('AsPredicted2')||")", middle)
                     else regexpr("("||var('AsPredicted3')||")", middle||next)
                     end as id
        from (
                select docid, prev, middle, next
                from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('AsPredicted1'))
                        from (select docid, text from mydata))
              )
        where regexprmatches(var('AsPredicted2'), middle) = 1 or
        regexprmatches(var('AsPredicted3'), middle||next) = 1
)
union all
select jdict('query', 'ANZCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
        select docid, prev, middle, next,
               case when regexprmatches(var('ANZCTR2'), middle) then regexpr("("||var('ANZCTR2')||")", middle)
                    else regexpr("("||var('ANZCTR3')||")", middle||next)
                end as id
        from ( select docid, prev, middle, next
                from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ANZCTR1'))
                        from (select docid, text from mydata))
             )
        where regexprmatches(var('ANZCTR2'), middle) = 1 or
              regexprmatches(var('ANZCTR3'),middle||next) = 1
)
union all
select jdict('query', 'ReBec', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('ReBec')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ReBec'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ChiCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
        select docid, prev, middle, next,
               case when regexprmatches(var('ChiCTR2'), middle) then regexpr("("||var('ChiCTR2')||")", middle)
                    else regexpr("("||var('ChiCTR3')||")", middle||next)
                end as id
        from ( select docid, prev, middle, next
                 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ChiCTR1'))
                        from (select docid, text from mydata))
              )
        where
        regexprmatches(var('ChiCTR2'), middle) = 1 or
        regexprmatches(var('ChiCTR3'),middle||next) = 1
)
union all
select jdict('query', 'CRiS', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
        select docid, prev, middle, next,
               case when regexprmatches(var('CRiS2'), middle) then regexpr("("||var('CRiS2')||")", middle)
                    else regexpr("("||var('CRiS3')||")", middle||next)
                end as id
        from ( select docid, prev, middle, next
                 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CRiS1'))
                        from (select docid, text from mydata))
              )
        where
        regexprmatches(var('CRiS2'), middle) = 1 or
        regexprmatches(var('CRiS3'),middle||next) = 1
)
union all
select jdict('query', 'CTIS', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id',  regexpr("("||var('CTIS')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CTIS'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'CTRI', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
        select docid, prev, middle, next,
               case when regexprmatches(var('CTRI2'), middle) then regexpr("("||var('CTRI2')||")", middle)
                    else regexpr("("||var('CTRI3')||")", middle||next)
                end as id
        from ( select docid, prev, middle, next
                from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CTRI1'))
                        from (select docid, text from mydata))
              )
        where
        regexprmatches(var('CTRI2'), middle) = 1 or
        regexprmatches(var('CTRI3'),middle||next) = 1
)
union all
select jdict('query', 'RPCEC', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('RPCEC')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('RPCEC'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'LTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('LTR')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('LTR'))
                from (select docid, text from mydata))
        where regexprmatches(var('LTR_positivewords'), lower(prev||middle||next)) = 1
)
union all
select jdict('query', 'EU_CTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next,'id', regexpr("("||var('EU_CTR')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('EU_CTR'))
                from (select docid, text from mydata))
        where  regexprmatches(var('EU_CTR_positivewords'), prev||middle||next) = 1
)
union all
select jdict('query', 'CT_gov', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('CT_gov')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CT_gov'))  from mydata)
        where regexprmatches('[A-Z1-9]NCT\s{0,1}\d{6,}',middle) = 0 and length(regexpr("(NCT\s{0,1}\d+)", middle))>=7
)
union all
select jdict('query', 'DRKS', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('DRKS')||")", middle) )
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('DRKS'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ICTRP', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next,'id', regexpr("("||var('ICTRP')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ICTRP'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'INPLASY', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next,'id', regexpr("("||var('INPLASY')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('INPLASY'))
                from (select docid, lower(text) as text from mydata))
)
union all
select jdict('query', 'PROSPERO', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
        select docid, prev, middle, next,
               case when regexprmatches(var('PROSPERO1'), middle||next) then regexpr(var('PROSPERO1b'), middle||next)
                    else regexpr("("||var('PROSPERO2')||")", middle)
                end as id
         from ( select docid, prev, middle, next
                 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('PROSPERO'))
                          from (select docid, text from mydata))
              )
         where ( regexprmatches('prospero\/display_record', middle) = 1 and regexprmatches(var('PROSPERO1'), middle||next) = 1 )
               or ( regexprmatches('prospero\/display_record', middle) = 0 and length(regexpr("\bCRD\s{0,1}(\d{6,})\b", middle))>= 8 )
               or ( regexprmatches('prospero\/display_record', middle) = 0 and regexprmatches('grant',lower(prev||middle||next)) = 0 and length(regexpr("\bCRD\s{0,1}(\d{6,})\b", middle))>=6 and length(regexpr("\bCRD\s{0,1}(\d{6,})\b", middle)) <=7)
)
union all
select jdict('query', 'ISRCTN', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id',  regexpr("("||var('ISRCTN')||")", middle))
from ( select docid, prev, middle, next
       from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ISRCTN'))
                 from (select docid, text from mydata))
)
union all
select jdict('query', 'ITMCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id',  regexpr("("||var('ITMCTR')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ITMCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'IRCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('IRCT')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('IRCT'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'JMACCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next,'id', regexpr("("||var('JMACCT')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('JMACCT'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'JAPIC', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('JAPIC')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('JAPIC'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'jRCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('jRCT')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('jRCT'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'LBCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('LBCTR')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('LBCTR'))
                 from (select docid, text from mydata))
)
union all
select jdict('query', 'OSF', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr(var('OSF1'), middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('OSF'))
                from (select docid, lower(text) as text from mydata))
)
union all
select jdict('query', 'PACTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
        select docid, prev, middle, next,
                  case when regexprmatches(var('PACTR2'), middle)
                       then regexpr("("||var('PACTR2')||")", middle)
                       else regexpr("("||var('PACTR3')||")", middle||next)
                       end as id
          from ( select docid, prev, middle, next
                 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('PACTR'))
                           from (select docid, text from mydata))
                )
          where  regexprmatches(var('PACTR2'), middle) = 1 or
                 regexprmatches(var('PACTR3'), middle||next) = 1
)
union all
select jdict('query', 'REPEC', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id' , middle)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('REPEC'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'PCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('PCT')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('PCT'))
                 from (select docid, text from mydata))
)
union all
select jdict('query', 'ResearchRegistry', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', id)
from (
          select docid, prev, middle, next,
                    case when regexprmatches(var('ResearchRegistry2'), middle)
                         then regexpr("("||var('ResearchRegistry2')||")", middle)
                         else regexpr("("||var('ResearchRegistry1')||")", middle||next)
                         end as id
            from ( select docid, prev, middle, next
                   from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ResearchRegistry'))
                             from (select docid, text from mydata))
                  )
            where  regexprmatches(var('ResearchRegistry2'), middle) = 1 or
                   regexprmatches(var('ResearchRegistry1'), middle||next) = 1
)
union all
select jdict('query', 'SLCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('SLCTR')||")", middle))
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('SLCTR'))
                from (select docid, lower(text) as text from mydata))
)
union all
select jdict('query', 'TCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next , 'id', regexpr("("||var('TCTR')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('TCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'UMIN', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next, 'id', regexpr("("||var('UMIN')||")", middle))
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('UMIN'))
                from (select docid, text from mydata))
);
