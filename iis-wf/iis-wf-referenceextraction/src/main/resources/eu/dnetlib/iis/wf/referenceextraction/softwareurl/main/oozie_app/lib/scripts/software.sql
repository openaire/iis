select jdict('documentId', id, 'SoftwareURL', match,'confidenceLevel',0.8) from (
select distinct id,regexpr("\W+$",match,"") as match from (select id,jfilterempty(jset(regexpr("((http\:\/\/code\.google\.com\/p\/(?:\w+|\-))|(https\:\/\/github.com\/.+\/(?:\w+|\-))|(https\:\/\/bitbucket.org\/.+\/.+)|(http://sourceforge.net/projects/.+))",middle))) as match from 
    (select c1 as id,textwindow2s(regexpr("\n",c2," "),0,1,0,"(http\:\/\/code\.google\.com\/p\/(\w+|\-))|(https\:\/\/github.com\/.+\/(\w+|\-))|(https\:\/\/bitbucket.org\/.+\/.+)|(http://sourceforge.net/projects/.+)") from 
    (select * from (setschema 'c1,c2' select jsonpath(c1, '$.id', '$.text') from stdinput()))
)));