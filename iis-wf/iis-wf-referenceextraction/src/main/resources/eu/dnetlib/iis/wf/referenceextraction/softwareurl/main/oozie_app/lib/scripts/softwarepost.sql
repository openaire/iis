PRAGMA temp_store_directory = '.';

select
jdict('documentId', id, 'softwareURL', match,'repositoryName',comprspaces(regexpr("\n",repo," ")),'softwareTitle',
comprspaces(regexpr("\n",title," ")),'softwareDescription',comprspaces(regexpr("\n",description," ")),'softwarePageURL',url,'confidenceLevel',0.8) from
(select id,match,repo,case when title is null then "" else title end as title,case when description is null then "" else description end as description,url from 
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
end as url
from (setschema 'id,match,repo,source' select jsonpath(c1, '$.documentId', '$.softwareUrl','$.repositoryName','$.Source') from stdinput())));

