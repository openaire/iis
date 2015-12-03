#!/bin/bash
exec 3>&1
BASH_XTRACEFD=3
set -x ## print every executed command


if [ $# = 0 ] ; then
    target_dir_root=`pwd`'/${oozieAppDir}'
else
    target_dir_root=`readlink -f $1`'/${oozieAppDir}'
fi

# initial phase, creating symbolic links to jars in all subworkflows
# currently disabled
#libDir=$target_dir_root'/lib'
#dirs=`find $target_dir_root/* -maxdepth 10 -type d`
#for dir in $dirs
#do
#        if [ -f $dir/workflow.xml ]
#        then
#                echo "creating symbolic links to jars in directory: $dir/lib"
#                if [ ! -d "$dir/lib" ]; then
#                        mkdir $dir/lib
#                fi
#                find $libDir -type f -exec ln -s \{\} $dir/lib \;
#        fi
#done


#uploading
hadoop fs -rm -r ${sandboxDir}
hadoop fs -mkdir -p ${sandboxDir}
hadoop fs -mkdir -p ${workingDir}
hadoop fs -put $target_dir_root ${sandboxDir}
