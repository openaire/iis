#!/usr/bin/env python

from __future__ import print_function

__author__ = "Mateusz Kobos"

import argparse
import subprocess
import avroknife.operations
import avroknife.data_store
import avroknife.file_system
from string import Template

load_table_template = Template("""DROP TABLE $name;

CREATE EXTERNAL TABLE $name
COMMENT "A table backed by Avro data with the Avro schema stored in HDFS"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '$path'
TBLPROPERTIES ('avro.schema.literal'='$schema');""")

def parse():
    """Parse CLI arguments"""
    parser = argparse.ArgumentParser(
        description="Generate Hive script to load selected Avro data stores "+
            "into Hive metastore.")
    parser.add_argument('inputs', metavar='input', nargs='+',
        help='Input data store of the form "table_name=HDFS_data_store_path", '+
            'e.g. user=some/cluster/path')
    args = parser.parse_args()
    return args

def run_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    output = p.stdout.read()
    if p.wait() != 0:
    	raise Exception("Calling command \"{}\" failed".format(command))
    return output

def get_schema(data_store_path):
    path = avroknife.file_system.HDFSPath(data_store_path)
    data_store = avroknife.data_store.DataStore(path)
    schema = avroknife.operations.get_schema(data_store)
    return schema

def abs_HDFS_path(path):
    """Convert path relative to user's home to absolute path.

    The relative path is the one not starting with "/" and the absolute one
    starts with "/". Note that is a hack, since to construct the absolute path
    we use user name from the local system, but I wasn't able to find
    a better solution."""

    if path.startswith("/"):
        return path
    user_name = run_command("whoami").strip()
    return "/user/{}/{}".format(user_name, path)

def main():
    args = parse()
    load_script = []
    for input_ in args.inputs:
    	elements = input_.split("=")
    	if len(elements) != 2:
            raise Exception("Wrong format of the input parameter "\
                "\"{}\"".format(input_))
    	(table_name, data_store_path_raw) = elements
        data_store_path = abs_HDFS_path(data_store_path_raw) 
        schema = get_schema(data_store_path)
        load_sql = load_table_template.substitute(
            name=table_name, path=data_store_path, 
            schema="{}".format(schema))
        load_script.append(load_sql)
    print("\n\n".join(load_script))

if __name__ == "__main__":
    main()
