create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());
--create table mydata as select * from (setschema 'docid,text' select jsonpath(c1, 'id','text') from (file  header:f 'mydata.txt' ));
select jdict('documentId', docid,'projectId', id, 'fundingclass1', fundingclass1, 'grantid', grantid, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (
        select docid, prev, middle,next, regexpr("(\d{4}- ?\d- ?[A-Z]{2}\d{2}- ?[A-Z]{2}\d{3}- ?[A-Z]{3}- ?[\d]+$)",prev||middle) as regexpr_prev_middle, id, grantid,fundingclass1
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, "\d{9}") from mydata)
        join
        (setschema 'id,grantid,fundingclass1' select jsonpath(C1,"id","grantid","fundingclass1") from erasmusjsoninput)
        on (middle = id  or regexpr_prev_middle = id)
        where  regexprmatches("funde?d?|project|erasmus", lower(prev)) = 1 );