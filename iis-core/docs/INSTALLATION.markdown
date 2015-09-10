In order to run the application, you need to have Cloudera's Hadoop installed. Among its facilities, you need to have Oozie installed as well; installing HBase might not be needed in your case, unless you specifically know that you will be using it. The steps of installation and configuration of Hadoop and its facilities are given below.

---

**Warning**: the description below is outdated. For example, the most recent version of Hadoop and Oozie work without any problems with Java 1.7. 

---

Hadoop
======
Java environment
----------------
IMPORTANT: Because of a bug in the Oozie version provided with Cloudera's Hadoop (by the way: this bug is removed in the version of Oozie available in the source code repository), **you need to have Oracle Java JDK 1.6 installed**. Oozie **does not** work with JDK 1.7.

Note that in order to install and configure Hadoop, you need to have `JAVA_HOME` environment variable set up properly. If you haven't got it set up already, you can do it by, e.g. adding a new line with contents `JAVA_HOME="/usr/lib/jvm/default-java"` (assuming that this is a proper path to installation directory of your Java distribution) to your `/etc/environment` file.


General information on Hadoop
-----------------------------
The instructions below show how to install Cloudera Hadoop CDH4 with MRv1 in accordance with the instructions given in [Cloudera CDH4 intallation guide](https://ccp.cloudera.com/display/CDH4DOC/CDH4+Installation+Guide).

It is important to know that Hadoop can be run in one of three modes:

- **standalone mode** - runs all of the Hadoop processes in a single JVM which makes it easy to debug the application. 
- **pseudo-distributed mode** - runs a full-fledged Hadoop on your local computer.
- **distributed mode** - runs the application on a cluster consisting of many nodes/hosts.

Below we will show how to install Hadoop initially in the pseudo-distributed mode but with a possibility to switch between the standalone and the pseudo-distributed mode.

Installation
------------
Installing Hadoop in pseudo-distributed mode (based on [Cloudera CDH4 pseudo distributed mode installation guide](https://ccp.cloudera.com/display/CDH4DOC/Installing+CDH4+on+a+Single+Linux+Node+in+Pseudo-distributed+Mode)) in case of 64-bit Ubuntu 12.04:

- create a new file `/etc/apt/sources.list.d/cloudera.list` with contents:

		deb [arch=amd64] http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh precise-cdh4 contrib
		deb-src [arch=amd64] http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh precise-cdh4 contrib

- add a repository key:
		
		curl -s http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh/archive.key | sudo apt-key add -
		
- update
			
		sudo apt-get update
	
- install packages 
			
		sudo apt-get install hadoop-0.20-conf-pseudo
			
- next, follow the steps described in the Cloudera's guide to installing Hadoop in the pseudo-distributed mode starting from the step "Step 1: Format the NameNode." This is available at [Cloudera CDH4 pseudo distributed mode installation guide - "Step 1: Format the Namenode"](https://ccp.cloudera.com/display/CDH4DOC/Installing+CDH4+on+a+Single+Linux+Node+in+Pseudo-distributed+Mode#InstallingCDH4onaSingleLinuxNodeinPseudo-distributedMode-Step1%3AFormattheNameNode.).
		
After install
-------------

### Switching between Hadoop modes
When you have Hadoop installed, you can **switch between standalone and pseudo-distributed configurations** (or other kinds of configurations) of Hadoop using the `update-alternatives` command, e.g.:

- `update-alternatives --display hadoop-conf` for list of available configurations and information which one is currently active
- `sudo update-alternatives --set hadoop-conf /etc/hadoop/conf.empty` to set the active configuration to `/etc/hadoop/conf.empty` which corresponds to Hadoop standalone mode.

### Web interfaces
You can view the web interfaces to the following services using appropriate addresses:

- **NameNode** - provides a web console for viewing HDFS, number of Data Nodes, and logs - [http://localhost:50070/](http://localhost:50070/)
	- In the pseudo-distributed configuration, you should see one live DataNode named "localhost".
- **JobTracker** - allows viewing the completed, currently running, and failed jobs along with their logs - [http://localhost:50030/](http://localhost:50030/)

Oozie
=====

Installation
------------

The description below is based on [Cloudera CDH4 Oozie installation guide](https://ccp.cloudera.com/display/CDH4DOC/Oozie+Installation#OozieInstallation-ConfiguringOozieinstall).

- Install Oozie with

		sudo apt-get install oozie oozie-client

- Create Oozie database schema

		sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run
	

	- this should result in an output similar to this one:

			Validate DB Connection
			DONE
			Check DB schema does not exist
			DONE
			Check OOZIE_SYS table does not exist
			DONE
			Create SQL schema
			DONE
			Create OOZIE_SYS table
			DONE
		
			Oozie DB has been created for Oozie version '3.1.3-cdh4.0.1'

			The SQL commands have been written to: /tmp/ooziedb-8221670220279408806.sql

- Install version 2.2 of ExtJS library:
	- download the zipped library from [http://extjs.com/deploy/ext-2.2.zip](http://extjs.com/deploy/ext-2.2.zip)
	- copy the zip file to `/var/lib/oozie` end extract it there
- Install Oozie ShareLib:

		mkdir /tmp/ooziesharelib
		cd /tmp/ooziesharelib
		tar -zxf /usr/lib/oozie/oozie-sharelib.tar.gz
		sudo -u hdfs hadoop fs -mkdir /user/oozie
		sudo -u hdfs hadoop fs -chown oozie /user/oozie
		sudo -u oozie hadoop fs -put share /user/oozie/share
	
- Start the Oozie server:

		sudo service oozie start

- Check the status of the server:
	- Using **command-line**:

			oozie admin -oozie http://localhost:11000/oozie -status
	
	as a result, the following should be printed out:

			System mode: NORMAL
			
	If instead of this output you get the exception `java.lang.NullPointerException` thrown, try executing the same command with `-auth SIMPLE` arguments, namely:
	
		oozie admin -auth SIMPLE -oozie http://localhost:11000/oozie -status
	
	This is related to a certain [Jira issue](https://issues.apache.org/jira/browse/OOZIE-1010). It seems that you have to use arguments `-auth SIMPLE` only once - after using them for the first time, the `oozie` program will no longer require them.
	
	- You can also check the status of the server using **web interface** - use a web browser to open a webpage at the following address: [http://localhost:11000/oozie/](http://localhost:11000/oozie/)

If you want to check if Oozie correctly executes its workflows, you can run some of the example workflows provided with Oozie as described in [Cloudera Oozie example workflows](http://archive.cloudera.com/cdh4/cdh/4/oozie/DG_Examples.html). Note that contrary to what is written there, the Oozie server is not available at `http://localhost:8080/oozie` but at `http://localhost:11000/oozie` address.

Documentation
-------------
A documentation about Oozie and creating Oozie workflows can be found at [Cloudera Oozie documentation](http://archive.cloudera.com/cdh4/cdh/4/oozie/).

A quite nice 3-part tutorial can be found at:

- [1. Introduction to Oozie at InfoQ](http://www.infoq.com/articles/introductionOozie)
- [2. Oozie by example at InfoQ](http://www.infoq.com/articles/oozieexample)
- [3. Extending Oozie at InfoQ](http://www.infoq.com/articles/ExtendingOozie)

HBase
=====
The description below is based on [Cloudera CDH4 HBase installation guide](https://ccp.cloudera.com/display/CDH4DOC/HBase+Installation).
The main goal is to have Hbase installed and working in pseudo-distributed mode utilizing hdfs of previously installed Hadoop instance.

Notice: on ubuntu systems installing packages listed below will start-up services straight away.

- Install HBase package

		sudo apt-get install hbase

- Install HBase Master package

		sudo apt-get install hbase-master

- Stop HBase Master in order to reconfigure HBase to work in pseudo-distributed mode

		sudo service hbase-master stop

- Configure HBase in pseudo-distributed mode by modifying /etc/hbase/conf/hbase-site.xml HBase configuration file and inserting:

		<property>
		  <name>hbase.cluster.distributed</name>
		  <value>true</value>
		</property>
		<property>
		  <name>hbase.rootdir</name>
		  <value>hdfs://localhost:8020/hbase</value>
		</property>

between the <configuration> and </configuration> tags. 

- Creating the /hbase Directory in HDFS with proper permissions

		sudo -u hdfs hadoop fs -mkdir /hbase
		sudo -u hdfs hadoop fs -chown hbase /hbase

where HBase data will be stored.

- Install Zookeper required for pseudo-distributed mode

		sudo apt-get install zookeeper-server

At first zookeper won't start due to the missing data directory. It will be created at init phase, therefore run:

		sudo service zookeeper-server init
		sudo service zookeeper-server start

- Inspect HBase Master administration panel in order to verify all required services are running. Default address is:

		http://localhost:60010/master-status

At least one region server should be listed. Default HBase home directory location is:

		hdfs://localhost:8020/hbase

- Access HBase by using the HBase Shell

		hbase shell

More detailed shell commands description are available on [http://wiki.apache.org/hadoop/Hbase/Shell](http://wiki.apache.org/hadoop/Hbase/Shell)

Troubleshooting
According to [http://hbase.apache.org/book.html#loopback.ip](http://hbase.apache.org/book.html#loopback.ip) 127.0.1.1 entry on ubuntu /etc/hosts file:

		127.0.1.1      laptop-work

may cause problems when deploying HBase Master with Region Server. The following exceptions:

		org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed setting up proxy interface org.apache.hadoop.hbase.ipc.HRegionInterface to localhost/127.0.0.1:60020 after attempts=1

may occur in hbase-master log file. As a solution: 127.0.1.1 entry should be removed from /etc/hosts file.
