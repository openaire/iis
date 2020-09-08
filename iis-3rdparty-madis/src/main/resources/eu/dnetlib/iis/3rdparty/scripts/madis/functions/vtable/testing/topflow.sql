var 'v' 5;
var 'tablename' 'internaltable';
var 'lastdate' from select '2008-01-01';
create table topflowvars as select * from getvars() where variable!='execdb';
exec 'tablename' 'c:v'  file 'internalflow.sql';