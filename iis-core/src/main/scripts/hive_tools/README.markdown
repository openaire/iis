Script `hive_datastores_load.py`
--------------------------------
Requirements of the script:

- `avroknife` Python package installed in the system (you can install it by executing `pip install avroknife`)
- Note that as of Hive version `0.10.0-cdh4.3.1`, there is a limit on the maximal length of schema of loaded Avro data source. The problem is that the JSON schema that is stored in database backing Hive's metastore is held in a field (`TABLE_PARAMS.PARAM_VALUE`) that has a limit of size of 4k characters. Unfortunately this is not enough for more complicated Avro schemas, so you have to manually modify the schema and raise the limit.

Example usages:

- `hive_datastores_load.py doc_meta=/share/import/doc_meta/2013-07-10 project=/share/import/project/2013-07-18`
- example one-liner to generate the load script, open the Hive console and execute the script: `hive_datastores_load.py doc_meta=/share/import/doc_meta/2013-07-10 > load.sql && hive -i load.sql`
