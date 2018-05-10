create table hep as setschema 'c1,c2' select jsonpath(c1,"id","label") from stdinput();
