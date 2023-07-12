create table services as select sid, name, regexpr("http://|https://|www\.|/$",url,"") as url from (setschema 'sid,name,url' select jsonpath(c1, 'id', 'name', 'url') from stdinput());
