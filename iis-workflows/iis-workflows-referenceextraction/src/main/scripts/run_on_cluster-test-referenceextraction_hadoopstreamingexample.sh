#!/bin/bash

mvn clean package -Pattach-test-resources,oozie,deploy -Dworkflow.source.dir=eu/dnetlib/iis/referenceextraction/hadoopstreamingexample
