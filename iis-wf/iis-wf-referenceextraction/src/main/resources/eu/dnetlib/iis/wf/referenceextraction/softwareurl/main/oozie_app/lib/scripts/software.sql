PRAGMA temp_store_directory = '.';

create temp table pubs as setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput();





select jdict('documentId', id, 'softwareURL', match,'repositoryName',repo,'softwareTitle',title,'softwareDescription',description,'softwarePageURL',url,'confidenceLevel',0.8) from (
select id, match, repo,
case
	when repo='GitHub' then regexpr('itemprop="name">.+>(.+)</a>', data)
	when repo='Bitbucket' then regexpr('bitbucket.org/[\w-]+/([\w-]+)/?', match)
	when repo='SourceForge' then regexpr('<meta property="og:title" content="(.+)"/>', data)
	when repo='Google Code' then 
		case 
			when regexprmatches('<meta name="hostname" content="github.com">', data) then regexpr('<strong itemprop="name">.+>(.+)</a>', data)
			else regexpr('itemprop="name">(.+)</span>', data)
		end
end as title,
case
	when repo='GitHub' then regexpr('itemprop="about">(.+?(?=</span>))', regexpr("\n",data," "))
	when repo='SourceForge' then regexpr('<meta property="og:description" content="(.+)"/>', data)
	when repo='Google Code' then 
		case 
			when regexprmatches('<meta name="hostname" content="github.com">', data) then regexpr('itemprop="about">(.+?(?=</span>))', regexpr("\n",data," "))
			else regexpr('<span itemprop="description">(.+)</span>', data)
		end
end as description,
case
	when repo='GitHub' then regexpr('<link rel="canonical" href="(.+)"', data)
	when repo='SourceForge' then regexpr('<meta property="og:url" content="(.+)"/>', data)
end as url
from (select id,match,repo, case when data is null then "" else data end as data from (
select id,match,
case
when regexprmatches("https*\:\/\/github.com",match) then "GitHub"
when regexprmatches("https*\:\/\/bitbucket.org\/",match) then "Bitbucket"                  
when regexprmatches("https*\:\/\/code\.google\.com",match) then "Google Code"
when regexprmatches("https*\:\/\/sourceforge\.net",match) then "SourceForge"
end as repo,
urlrequest(null, unitosuni(match)) as data
from 
(select distinct id,regexpr("\W+$",match,"") as match
 from (select id,jfilterempty(jset(regexpr("((https*\:\/\/code\.google\.com\/p\/(?:\w+|\-))|(https*\:\/\/github.com\/.+\/(?:\w+|\-))|(https*\:\/\/bitbucket.org\/.+\/.+)|(https*\:\/\/sourceforge\.net\/projects\/.+))",middle))) as match from 
    (setschema 'id,prev,middle,next' select c1 as id,textwindow2s(regexpr("\n",c2," "),0,1,0,"(https*\:\/\/code\.google\.com\/p\/(\w+|\-))|(https*\:\/\/github.com\/.+\/(\w+|\-))|(https*\:\/\/bitbucket.org\/.+\/.+)|(https*\:\/\/sourceforge\.net\/projects\/.+)") from 
(setschema 'c1,c2' select * from pubs where c2 is not null and c1 is not null)
))))));
