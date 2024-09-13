--For testing...
-- cd /storage/eleni/openAIRE/UKNR
-- attach database "/storage/eleni/openAIRE/06.Biomedical06/mydata.db" as d1;
-- create table mydata as select * from (setschema 'docid,text' select * from mydata where
--              docid ='PMC2931525' or  docid ='PMC2933899' or
--              docid ='PMC3737084' or  docid ='PMC3737070' );
-- output 'pubs.txt' select jdict('id', docid, 'text', text) from mydata;
--
--cp pubs.txt pubs.json
--cat pubs.json | python ~/Desktop/openAIRE/madis2/src/mexec.py -f pilot6.sql -d test01.db > results_v1.json

--cat pubs_empty.json | python ~/Desktop/openAIRE/madis2/src/mexec.py -f pilot6.sql -d test01.db > results_v2.json


create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());

hidden var 'ASR' from "www\.animalstudyregistry\.org|\b10\.17590\/asr\.\d+\b";
hidden var 'AsPredicted' from "aspredicted\.org|\bAsPredicted\s{0,1}#\d+\b";
hidden var 'ANZCTR' from "www\.anzctr\.org\.au|\bACTRN:{0,1}\s{0,1}\d+p{0,1}\b";
hidden var 'ReBec' from "ensaiosclinicos\.gov\.br|\bRBR-\d+[a-z0-9]+\b";
hidden var 'ChiCTR' from "www\.chictr\.org\.cn|\bChiCTR\s{0,1}-{0,1}(?:TRC){0,1}-{0,1}\d+\b";
hidden var 'CRiS' from "cris\.nih\.go\.kr\cris|\bKCT\s{0,1}\d{7}\b";
hidden var 'CTIS' from "euclinicaltrials\.eu|\bEUCT\s{0,1}\d{4}-\d+-\d{2}-\d{2}\b";
hidden var 'CTRI' from "ctri\.nic\.in|\bCTRI/\d{4}/\d{2}/\d+\b";
hidden var 'CT_gov' from "clinicaltrials\.gov|\bNCT\s{0,1}\d+\b";
hidden var 'RPCEC' from "rpcec\.sld\.cu|\bRPCEC\s{0,1}\d+\b";
hidden var 'LTR' from "www\.onderzoekmetmensen\.nl|\bNL\s{0,1}\d+ ; NTR\s{0,1}\d+\b";
hidden var 'EU_CTR' from "www\.clinicaltrialsregister\.eu";
hidden var 'DRKS' from "drks\.de|\bDRKS\s{0,1}\d+\b";
hidden var 'ICTRP' from "trialsearch\.who\.int|\bU\d{4}-\d{4}-\d{4}\b";
hidden var 'INPLASY' from "inplasy\.com|\bINPLASY\s{0,1}\d+\b";
hidden var 'PROSPERO' from "www\.crd\.york\.ac\.uk\/prospero|\bCRD\s{0,1}\d+\b";
hidden var 'ISRCTN' from "www\.isrctn\.com|\bISRCTN\s{0,1}\d+\b";
hidden var 'ITMCTR' from "itmctr\.ccebtcm\.org\.cn|\bITMCTR\s{0,1}\d+\b";
hidden var 'IRCT' from "www\.irct\.ir|\bIRCT\s{0,1}\d+\b";
hidden var 'JMACCT' from "rctportal\.niph\.go\.jp|\bJMA-IIA\s{0,1}\d+\b";
hidden var 'JAPIC' from "www\.clinicaltrials\.jp|\bJapicCTI\s{0,1}-{0,1}\d+\b";
hidden var 'jRCT' from "rctportal\.niph\.go\.jp|\bjRCTs{0,1}\s{0,1}\d+\b";
hidden var 'LBCTR' from "lbctr\.moph\.gov\.lb|\bLBCTR\s{0,1}\d+\b";
hidden var 'OSF' from "osf\.io/search\?resourceType=Registration";
hidden var 'PACTR' from "pactr\.samrc\.ac\.za|\bPACTR\s{0,1}\d+\b";
hidden var 'REPEC' from "ensayosclinicos-repec\.ins\.gob\.pe|\bPER-\d+-\d+\b";
hidden var 'PCT' from "preclinicaltrials\.eu|\bPCTE\s{0,1}\d+\b";
hidden var 'ResearchRegistry' from "www\.researchregistry\.com|\bresearchregistry\s{0,1}\d+\b";
hidden var 'SLCTR' from "www\.slctr\.lk|\bSLCTR/\d{4}/\d+\b";
hidden var 'TCTR' from "thaiclinicaltrials\.org|\bTCTR\s{0,1}\d+\b";
hidden var 'UMIN' from "ww\.umin\.ac\.jp|\bUMIN\s{0,1}\d+\b";


select jdict('query', 'ASR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ASR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'AsPredicted', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('AsPredicted'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ANZCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ANZCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ReBec', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ReBec'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ChiCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ChiCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'CRiS', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CRiS'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'CTIS', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CTIS'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'CTRI', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CTRI'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'CT_gov', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('CT_gov'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'RPCEC', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('RPCEC'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'LTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('LTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'EU_CTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('EU_CTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'DRKS', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('DRKS'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ICTRP', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ICTRP'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'INPLASY', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('INPLASY'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'PROSPERO', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('PROSPERO'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ISRCTN', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ISRCTN'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ITMCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ITMCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'IRCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('IRCT'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'JMACCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('JMACCT'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'JAPIC', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('JAPIC'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'jRCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('jRCT'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'LBCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('LBCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'OSF', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('OSF'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'PACTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('PACTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'REPEC', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('REPEC'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'PCT', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('PCT'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'ResearchRegistry', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ResearchRegistry'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'SLCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('SLCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'TCTR', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('TCTR'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'UMIN', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('UMIN'))
                from (select docid, text from mydata))
);
