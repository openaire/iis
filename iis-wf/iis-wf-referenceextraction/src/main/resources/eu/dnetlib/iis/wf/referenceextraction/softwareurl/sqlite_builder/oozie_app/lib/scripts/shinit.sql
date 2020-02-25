create table links as select id, lower(link) as link, link as originlink from (setschema 'id,link' select rowid as id, c1 as link from (rowidvt select jsonpath(c1,'url') from stdinput()));
CREATE INDEX links_ind on links(link,originlink);

