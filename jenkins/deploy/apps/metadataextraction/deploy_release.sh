#!/bin/bash

# exitting on error
set -e
# printing commands
set -x

export deployment_date=`date +%Y-%m-%d`

# deployment parameters
app_dir="wf/metadataextraction/cache/builder"
hdfs_target_root_dir="/lib/iis/cache_builder/snapshots"

# gaining hdfs priviledges first in order to deploy oozie workflows to /lib/iis/ HDFS dir
user_name=`whoami`
export HADOOP_USER_NAME="hdfs"

# creating config-default.xml files in approparite local FS directories first
local_cfg_root_dir=${app_dir}'/config-default'
mkdir ${local_cfg_root_dir}/${deployment_date}
envsubst < ${local_cfg_root_dir}/config-default.template > ${local_cfg_root_dir}/${deployment_date}/config-default.xml

hdfs_app_source_dir='/user/'$user_name'/'${app_dir}'/oozie_app'
fs_cfg_source_dir=${local_cfg_root_dir}/${deployment_date}
hdfs_target_dir=${hdfs_target_root_dir}/${deployment_date}

# deploying app
hadoop fs -mv ${hdfs_app_source_dir} ${hdfs_target_dir}

# deploying config
hadoop fs -put ${fs_cfg_source_dir}/config-default.xml ${hdfs_target_dir}

export HADOOP_USER_NAME="$user_name"
