#!/bin/bash

mvn clean package -Pattach-test-resources,oozie,deploy-local -Dworkflow.source.dir=eu/dnetlib/iis/referenceextraction/project/bulk
