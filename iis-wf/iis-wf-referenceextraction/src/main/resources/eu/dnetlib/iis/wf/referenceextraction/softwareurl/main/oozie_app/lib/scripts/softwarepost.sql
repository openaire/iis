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


create temp table extractedpartiallinks as select id, regexpr("&gt",cleanmatch,"") as cleanmatch,match,repo from
(select distinct id,regexpr("\W+$",cleanmatch,"") as cleanmatch,match,repo,source from (select id,jfilterempty(jset(regexpr("(http.+)",match))) as cleanmatch,match, repo,source from metadata));

create temp table extractedlinks as select extractedpartiallinks.id, extractedpartiallinks.cleanmatch, extractedpartiallinks.match, extractedpartiallinks.repo, title, description, url  from extractedpartiallinks left join partial1 on extractedpartiallinks.id = partial1.id and extractedpartiallinks.match = partial1.match; 

update extractedlinks set title = comprspaces(regexpr("\n",title," ")) where title is not null and title != "";
update extractedlinks set cleanmatch = regexpr("http(s*):\/\/www\.github\.com",cleanmatch,"https://github.com") where cleanmatch is not null;
update extractedlinks set cleanmatch = regexpr("http\:\/\/github\.com",cleanmatch,"https://github.com") where cleanmatch is not null;
update extractedlinks set cleanmatch = lower(regexpr("\.git$",cleanmatch,"")) where cleanmatch is not null;
update extractedlinks set cleanmatch = regexpr("(https://github.com/[^/]+/[^/]+)",cleanmatch) where cleanmatch like "https://github.com/%/%/%";
update extractedlinks set description = comprspaces(regexpr("\n",description," ")) where description is not null and description != "";
update extractedlinks set repo = comprspaces(regexpr("\n",repo," ")) where repo is not null and repo != "";
update extractedlinks set title = regexpr("/([^/]+)$",cleanmatch) where title = "" or title is null;
update extractedlinks set title = "" where title is null;


select jdict('documentId', id, 'softwareUrl', match,'repositoryName',repo, 'cleanmatch', cleanmatch,'softwareTitle',
title,'softwareDescription',description, 'softwarePageURL',url,'confidenceLevel',0.8) from extractedlinks;


-- select extractedlinks.documentId,extractedlinks.repositoryName,extractedlinks.softwareUrl, extractedlinks.softwareTitle, extractedlinks.softwareDescription, extractedlinks.softwarePageURL, extractedlinks.confidenceLevel, "https://archive.softwareheritage.org/browse/origin/"||originlink as SHUrl from extractedlinks left join shlinks on extractedlinks.cleanmatch = lower(shlinks.link);
