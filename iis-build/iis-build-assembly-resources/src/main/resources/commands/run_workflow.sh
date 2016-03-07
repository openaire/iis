#!/bin/bash

if [ $# = 0 ] ; then
    oozie job -oozie ${oozieServiceLoc} -config job.properties -run
else
    oozie job -oozie ${oozieServiceLoc} -config $1/job.properties -run
fi



