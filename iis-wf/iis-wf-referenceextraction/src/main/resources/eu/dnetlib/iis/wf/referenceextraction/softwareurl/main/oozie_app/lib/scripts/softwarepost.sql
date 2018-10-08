PRAGMA temp_store_directory = '.';

create temp table metadata as setschema 'id,match,repo,source' select jsonpath(c1, '$.documentId', '$.softwareUrl','$.repositoryName','$.source') from stdinput();
create temp table partial1 as select * from
(select id,match,repo,case when title is null then "" else title end as title,case when description is null then "" else description end as description,url,SHUrl from 
(select id, match, repo,
case
        when repo='GitHub' then regexpr('itemprop="name">.+>(.+)</a>', source)
        when repo='Bitbucket' then regexpr('bitbucket.org/[\w-]+/([\w-]+)/?', match)
        when repo='SourceForge' then regexpr('<meta property="og:title" content="(.+)"/>', source)
        when repo='Google Code' then
                case
                        when regexprmatches('<meta name="hostname" content="github.com">', source) then regexpr('<strong itemprop="name">.+>(.+)</a>', source)
                        else regexpr('itemprop="name">(.+)</span>', source)
                end
end as title,
case
        when repo='GitHub' then regexpr('itemprop="about">(.+?(?=</span>))', regexpr("\n",source," "))
        when repo='SourceForge' then regexpr('<meta property="og:description" content="(.+)"/>', source)
        when repo='Google Code' then
                case
                        when regexprmatches('<meta name="hostname" content="github.com">', source) then regexpr('itemprop="about">(.+?(?=</span>))', regexpr("\n",source," "))
                        else regexpr('<span itemprop="description">(.+)</span>', source)
                end
end as description,
case
        when repo='GitHub' then regexpr('<link rel="canonical" href="(.+)"', source)
        when repo='SourceForge' then regexpr('<meta property="og:url" content="(.+)"/>', source)
end as url,"" as SHUrl
from metadata where repo = 'GitHub' or repo='SourceForge' or repo = 'Google Code' or repo = 'Bitbucket'));


create temp table links3 as select id, regexpr("&gt",match1,"") as match1,match,repo,source from
(select distinct id,regexpr("\W+$",match1,"") as match1,match,repo,source from (select id,jfilterempty(jset(regexpr("(http.+)",match))) as match1,match,repo,source from metadata));


update links3 set match1 = regexpr("http(s*):\/\/www\.github\.com",match1,"https://github.com") where match1 is not null;
update links3 set match1 = regexpr("http\:\/\/github\.com",match1,"https://github.com") where match1 is not null;
update links3 set match1 = lower(regexpr("\.git$",match1,"")) where match1 is not null;
update links3 set match1 = regexpr("(https://github.com/[^/]+/[^/]+)",match1) where match1 like "https://github.com/%/%/%";


create temp table matches as select links3.id,repo,links3.source,"https://archive.softwareheritage.org/browse/origin/"||match1 as match2,match from links3,links where links3.match1 = links.link;

create temp table partial2 as select id, match, repo, regexpr("(\w+)$",match2) as title, "" as description,"" as url, match2 as SHUrl  from matches;

update partial1 set SHUrl = (select SHUrl from partial2 where partial2.id||partial2.match=partial1.id||partial1.match) where partial1.id||partial1.match in (select partial2.id||partial2.match from partial2);
insert into partial1 select * from partial2 where partial2.id||partial2.match not in (select partial2.id||partial1.match from partial1);
update partial1 set title = (select comprspaces(title) from partial2 where partial2.match=partial1.match and title is not null and title != "") where title="" or title="null";
update partial1 set title="" where title is null;


select jdict('documentId', id, 'softwareUrl', match,'repositoryName',comprspaces(regexpr("\n",repo," ")),'softwareTitle',
comprspaces(regexpr("\n",title," ")),'softwareDescription',comprspaces(regexpr("\n",description," ")),'softwarePageURL',url,'SHUrl',SHUrl,'confidenceLevel',0.8) from partial1;
