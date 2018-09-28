create table links as select id,source, lower(link) as link from (setschema 'id,source,link' select rowid as id, c1 as source, c2 as link from (rowidvt select jsonpath(c1,'source','url') from stdinput()));
CREATE INDEX links_ind on links(link);
