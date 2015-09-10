Inside the subdirectories you'll find madIS' usage examples.

To execute the examples you'll need to enter inside a subdirectory and start
mterm with a database name argument:

> cd dblp
> mterm dblp.db

You can execute the entirety of a particular SQL flow by using:

exec flow file 'dblp.sql';

Or by opening "dblp.sql" in a text editor and copy pasting one by one the
SQL commands, from text editor's window into mterm's window.

To optimize the resulting database, it is best to execute (inside mterm) the 
following SQLite pragmas as soon as you open mterm, and before executing any
other SQL query:

pragma page_size = 16384;
pragma default_cache_size = 10000;
pragma journal_mode = PERSIST;
pragma journal_size_limit = 10000000;
pragma legacy_file_format = false;
pragma synchronous = 1;
pragma auto_vacuum = 2;

Above pragmas optimize SQLite's default options for analytic (bulk/OLAP) SQL queries.

