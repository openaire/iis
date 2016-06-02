select jdict('documentId', id, 'softwareUrl', match,'repositoryName',repo,'confidenceLevel',0.8) from (
select id,match,
case
when regexprmatches("https*\:\/\/github.com",match) then "GitHub"
when regexprmatches("https*\:\/\/bitbucket.org\/",match) then "Bitbucket"                  
when regexprmatches("https*\:\/\/code\.google\.com",match) then "Google Code"
when regexprmatches("https*\:\/\/sourceforge\.net",match) then "SourceForge"

end as repo

from 
(select distinct id,regexpr("\W+$",match,"") as match
 from (select id,jfilterempty(jset(regexpr("((https*\:\/\/code\.google\.com\/p\/(?:\w+|\-))|(https*\:\/\/github.com\/.+\/(?:\w+|\-))|(https*\:\/\/bitbucket.org\/.+\/.+)|(https*\:\/\/sourceforge\.net\/projects\/.+))",middle))) as match from 
    (setschema 'id,prev,middle,next' select c1 as id,textwindow2s(regexpr("\n",c2," "),0,1,0,"(https*\:\/\/code\.google\.com\/p\/(\w+|\-))|(https*\:\/\/github.com\/.+\/(\w+|\-))|(https*\:\/\/bitbucket.org\/.+\/.+)|(https*\:\/\/sourceforge\.net\/projects\/.+)") from 
(select * from (setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput()) where c2 is not null and c1 is not null)
))));
