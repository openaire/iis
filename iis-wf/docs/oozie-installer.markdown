General notes
====================

Oozie-installer is a utility allowing building, uploading and running oozie workflows. In practice, it creates a `*.tar.gz` package that contains resouces that define a workflow and some helper scripts.

This module is automatically executed when running: 

`mvn package -Poozie-package -Dworkflow.source.dir=classpath/to/parent/directory/of/oozie_app` 

on module having set:

	<parent>
            <groupId>eu.dnetlib</groupId>
            <artifactId>iis-wf</artifactId>
            <version>0.0.1-SNAPSHOT</version>
	</parent>

in `pom.xml` file. `oozie-package` profile initializes oozie workflow packaging, `workflow.source.dir` property points to a workflow (notice: this is not a relative path but a classpath to directory usually holding `oozie_app` subdirectory). 
 
The outcome of this packaging is `oozie-package.tar.gz` file containing inside all the resources required to run Oozie workflow:

- jar packages
- workflow definitions
- job properties
- maintenance scripts

Required properties
====================

In order to include proper workflow within package, `workflow.source.dir` property has to be set. It could be provided by setting `-Dworkflow.source.dir=some/job/dir` maven parameter.

In oder to define full set of cluster environment properties one should create `~/.iis/application.properties` file with the following properties:

- `iis.hadoop.frontend.user.name` - your user name on hadoop cluster and frontend machine
- `iis.hadoop.frontend.host.name` - frontend host name
- `iis.hadoop.frontend.temp.dir` - frontend directory for temporary files
- `iis.hadoop.frontend.port.ssh` - frontend machine ssh port
- `oozieServiceLoc` - oozie service location required by run_workflow.sh script executing oozie job
- `nameNode` - name node address
- `jobTracker` - job tracker address
- `oozie.execution.log.file.location` - location of file that will be created when executing oozie job, it contains output produced by `run_workflow.sh` script (needed to obtain oozie job id)
- `maven.executable` - mvn command location, requires parameterization due to a different setup of CI cluster
- `sparkDriverMemory` - amount of memory assigned to spark jobs driver
- `sparkExecutorMemory` - amount of memory assigned to spark jobs executors
- `sparkExecutorCores` - number of cores assigned to spark jobs executors

All values will be overriden with the ones from `job.properties` and eventually `job-override.properties` stored in module's main folder.

When overriding properties from `job.properties`, `job-override.properties` file can be created in main module directory (the one containing `pom.xml` file) and define all new properties which will override existing properties. One can provide those properties one by one as command line -D arguments.

Properties overriding order is the following:

1. `pom.xml` defined properties (located in the project root dir)
2. `~/.iis/application.properties` defined properties
3. `${workflow.source.dir}/job.properties`
4. `job-override.properties` (located in the project root dir)
5. `maven -Dparam=value`

where the maven `-Dparam` property is overriding all the other ones.

Workflow definition requirements
====================

`workflow.source.dir` property should point to the following directory structure:

	[${workflow.source.dir}]
		|
		|-job.properties (optional)
		|
		\-[oozie_app]
			|
			\-workflow.xml

This property can be set using maven `-D` switch.

`[oozie_app]` is the default directory name however it can be set to any value as soon as `oozieAppDir` property is provided with directory name as value. 

Subworkflows are supported as well and subworkflow directories should be nested within `[oozie_app]` directory. 

Creating oozie installer step-by-step
=====================================

Automated oozie-installer steps are the following:

1. creating jar packages:  `*.jar` and `*tests.jar` along with copying all dependancies in `target/dependencies`
2. reading properties from maven, `~/.iis/application.properties`, `job.properties`, `job-override.properties`
3. invoking priming mechanism linking resources from import.txt file (currently resolving subworkflow resources)
4. assembling shell scripts for preparing Hadoop filesystem, uploading Oozie application and starting workflow
5. copying whole `${workflow.source.dir}` content to `target/${oozie.package.file.name}`
6. generating updated `job.properties` file in `target/${oozie.package.file.name}` based on maven, `~/.iis/application.properties`, `job.properties` and `job-override.properties`
7. creating `lib` directory (or multiple directories for subworkflows for each nested directory) and copying jar packages created at step (1) to each one of them
8. bundling whole `${oozie.package.file.name}` directory into single tar.gz package

Uploading oozie package and running workflow on cluster
=======================================================

In order to simplify deployment and execution process two dedicated profiles were introduced:

- `deploy`
- `run`

to be used along with `oozie-package` profile e.g. by providing `-Poozie-package,deploy,run` maven parameters.

`deploy` profile supplements packaging process with:
1) uploading oozie-package via scp to `/home/${user.name}/oozie-packages` directory on `${iis.hadoop.frontend.host.name}` machine
2) extracting uploaded package
3) uploading oozie content to hadoop cluster HDFS location defined in `oozie.wf.application.path` property (generated dynamically by maven build process, based on `${iis.hadoop.frontend.user.name}` and `workflow.source.dir` properties)

`run` profile introduces:
1) executing oozie application uploaded to HDFS cluster using `deploy` command. Triggers `run_workflow.sh` script providing runtime properties defined in `job.properties` file.

Notice: ssh access to frontend machine has to be configured on system level and it is preferable to set key-based authentication in order to simplify remote operations.
