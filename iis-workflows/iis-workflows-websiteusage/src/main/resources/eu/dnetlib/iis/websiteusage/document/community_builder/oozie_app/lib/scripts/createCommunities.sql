drop table if exists piwiklog;
create table piwiklog (datetime, action,user,session,data);
insert into piwiklog select * from (setschema 'datetime, action,user,session,data' select jdictsplit(stripchars(c1,','),'timestamp','action','user','session','data') from stdinput());
create index piwiklog_index on piwiklog(action,data,session,user,datetime);


select jdict('cid',cliqueid,'SimilarDocid','50|'||nodeid) from 
    (select graphcliques(node1,node2) from 
        (select pgroup1 as node1, pgroup2 as node2, count(*) as frequency from 
            (setschema 'session,pgroup1,pgroup2' select session,jcombinations(jsort(jset(jgroup(data))),2) as pgroup from 
                (select datetime, action,user,session,jlengthiest(jdictvals(urlquery2jdict(data),'personId','projectId','publicationId','datasourceId','datasetId')) as data from piwiklog) 
                where (action='viewPerson' or action='viewPublication' or action='viewProject' or action='viewDatasource' or action='viewOrganization' or action='viewDataset') 
            group by session) 
        group by node1,node2 having frequency>4)
    ) 
where nodeid is not null;