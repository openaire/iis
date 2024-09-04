--For testing...
-- attach database "../06.Biomedical06/mydata.db" as d1;
-- create table mydata as select * from (setschema 'docid,text' select * from mydata where
--              docid ='PMC2931525' or  docid ='PMC2933899' or
--              docid ='PMC3737084' or  docid ='PMC3737070' );
-- output 'pubs.txt' select jdict('id', docid, 'text', text) from mydata;
--
--cp pubs.txt pubs.json
--cat pubs.json | python ~/Desktop/openAIRE/madis2/src/mexec.py -f pilot6.sql -d test01.db > results_v1.json

--cat pubs_empty.json | python ~/Desktop/openAIRE/madis2/src/mexec.py -f pilot6.sql -d test01.db > results_v2.json


create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());

hidden var 'urls' from "www\.animalstudyregistry\.org|aspredicted\.org|www\.anzctr\.org\.au|ensaiosclinicos\.gov\.br|www\.chictr\.org\.cn|cris\.nih\.go\.kr\cris|euclinicaltrials\.eu|ctri\.nic\.in|clinicaltrials\.gov|rpcec\.sld\.cu|www\.onderzoekmetmensen\.nl|www\.clinicaltrialsregister\.eu|drks\.de|trialsearch\.who\.int|inplasy\.com|www\.crd\.york\.ac\.uk\/prospero|www\.isrctn\.com|itmctr\.ccebtcm\.org\.cn|www\.irct\.ir|rctportal\.niph\.go\.jp|www\.clinicaltrials\.jp|rctportal\.niph\.go\.jp|lbctr\.moph\.gov\.lb|osf\.io/search\?resourceType=Registration|pactr\.samrc\.ac\.za|ensayosclinicos-repec\.ins\.gob\.pe|preclinicaltrials\.eu|www\.researchregistry\.com|www\.slctr\.lk|thaiclinicaltrials\.org|ww\.umin\.ac\.jp";

hidden var 'regexstatements' from
"10\.17590\/asr\.\d+|AsPredicted\s{0,1}#\d+|ACTRN:{0,1}\s{0,1}\d+p{0,1}|RBR-\d+[a-z0-9]+|ChiCTR\s{0,1}-{0,1}(?:TRC){0,1}-{0,1}\d+|KCT\s{0,1}\d{7}|EUCT\s{0,1}\d{4}-\d+-\d{2}-\d{2}|CTRI/\d{4}/\d{2}/\d+|NCT\s{0,1}\d+|RPCEC\s{0,1}\d+|NL\s{0,1}\d+ ; NTR\s{0,1}\d+|\d{4}[-–]\d+-\d+|DRKS\s{0,1}\d+|U\d{4}-\d{4}-\d{4}|INPLASY\s{0,1}\d+|CRD\s{0,1}\d+|ISRCTN\s{0,1}\d+|ITMCTR\s{0,1}\d+|IRCT\s{0,1}\d+|JMA-IIA\s{0,1}\d+|JapicCTI\s{0,1}-{0,1}\d+|jRCTs{0,1}\s{0,1}\d+|LBCTR\s{0,1}\d+|PACTR\s{0,1}\d+|PER-\d+-\d+|PCTE\s{0,1}\d+|researchregistry\s{0,1}\d+|SLCTR/\d{4}/\d+|TCTR\s{0,1}\d+|UMIN\s{0,1}\d+";

select jdict('query', 'a', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('urls'))
                from (select docid, text from mydata))
)
union all
select jdict('query', 'b', 'documentId', docid, 'prev', prev, 'middle', middle, 'next', next)
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('regexstatements'))
                from (select docid, text from mydata))
);