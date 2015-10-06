package eu.dnetlib.iis.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Base class for testing oozie workflows on cluster
 * 
 * @author madryk
 *
 */
public abstract class AbstractOozieWorkflowTestCase {
	
	private final static Logger log = LoggerFactory.getLogger(AbstractOozieWorkflowTestCase.class);
	
	
	private final static String OOZIE_SERVICE_LOC_KEY = "oozieServiceLoc";
	
	private final static String OOZIE_RUN_WORKFLOW_LOG_FILE_KEY = "oozie.execution.log.file.location";
	
	private final static String OOZIE_WORKFLOW_DEPLOY_MODE_KEY = "deploy.mode";
	
	private final static String NAME_NODE_KEY = "nameNode";
	
	private final static String WORKFLOW_SOURCE_DIR_KEY = "workflow.source.dir";
	
	private final static String MAVEN_TEST_WORKFLOW_PHASE = "clean package";
	
	private final static String MAVEN_TEST_WORKFLOW_LOCALLY_PROFILE = "attach-test-resources,oozie-package,deploy-local,run-local";
	
	private final static String MAVEN_TEST_WORKFLOW_REMOTELY_PROFILE = "attach-test-resources,oozie-package,deploy,run";
	

	private static IntegrationTestPropertiesReader propertiesReader;
	
	private OozieClient oozieClient;
	
	private FileSystem hadoopFilesystem;
	
	private OozieTestHelper oozieTestHelper;
	
	private HdfsTestHelper hdfsTestHelper;
	
	private File tempDir;
	
	
	private enum DeployMode {
		LOCAL,
		SSH
	}
	
	
	@BeforeClass
	public static void classSetUp() {
		propertiesReader = new IntegrationTestPropertiesReader();
		
	}
	
	@Before
	public void setUp() throws IOException {
		
		log.debug("Setting up OozieClient at {}", getOozieServiceLoc());
		oozieClient = new OozieClient(getOozieServiceLoc());
		
		Configuration hdfsConf = new Configuration(false);
		hdfsConf.set("fs.defaultFS", getNameNode());
		
		hadoopFilesystem = FileSystem.get(hdfsConf);
		
		oozieTestHelper = new OozieTestHelper(oozieClient);
		hdfsTestHelper = new HdfsTestHelper(hadoopFilesystem);
		
		tempDir = Files.createTempDir();
	}
	
	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(tempDir);
	}
	
	/**
	 * Tests workflow located under {@literal workflowPath} parameter.
	 * Internally uses {@link #testWorkflow(String, OozieWorkflowTestConfiguration)}
	 * with default {@link OozieWorkflowTestConfiguration}
	 */
	protected WorkflowTestResult testWorkflow(String workflowPath) {
		return testWorkflow(workflowPath, new OozieWorkflowTestConfiguration());
	}
	
	/**
	 * Tests workflow located under {@literal workflowPath} parameter.
	 * 
	 * @param workflowPath - Folder that contains workflow data.
	 *  	It must contain file {@literal oozie_app/workflow.xml} with workflow definition.
	 * @param configuration - some additional test pass criteria
	 *  	(for example timeout - a workflow must finish its execution before specified amount of time, 
	 *   	otherwise the test will automatically fail)
	 */
	protected WorkflowTestResult testWorkflow(String workflowPath, OozieWorkflowTestConfiguration configuration) {
		
		Process p = runMavenTestWorkflow(workflowPath);

		logMavenOutput(p);
		
		
		String jobId = OozieTestHelper.readJobIdFromLogFile(new File(getRunOoozieJobLogFilename()));
		
		
		Status jobStatus = waitForJobFinish(jobId, configuration.getTimeoutInSeconds());
		
		assertJobStatus(jobId, jobStatus, configuration.getExpectedFinishStatus());
		
		
		Properties jobProperties = oozieTestHelper.fetchJobProperties(jobId);
		String workflowWorkingDir = jobProperties.getProperty("workingDir");
		
		WorkflowTestResult result = new WorkflowTestResult();
		
		Map<String, File> workflowOutputFiles = hdfsTestHelper.importFilesFromHdfs(workflowWorkingDir, configuration.getExpectedOutputFiles(), tempDir);
		result.setWorkflowOutputFiles(workflowOutputFiles);
		
		Map<String, List<? extends SpecificRecord>> workflowOutputDataStores = 
				hdfsTestHelper.importAvroDatastoresFromHdfs( workflowWorkingDir, configuration.getExpectedOutputAvroDataStore());
		result.setWorkflowOutputAvroDataStores(workflowOutputDataStores);
		
		return result;
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private Process runMavenTestWorkflow(String workflowSource) {
		
		Process p;
		try {
			p = Runtime.getRuntime().exec("mvn " + MAVEN_TEST_WORKFLOW_PHASE + " -DskipTests "
					+ " -P" + chooseProfiles()
					+ " -D" + WORKFLOW_SOURCE_DIR_KEY + "=" + workflowSource
					);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return p;
	}
	
	private void logMavenOutput(Process p) {
		
		BufferedReader stdInput = new BufferedReader(new
				InputStreamReader(p.getInputStream()));
		try {
			String s = null;
			while ((s = stdInput.readLine()) != null) {
				System.out.println(s);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(stdInput);
		}
		

		BufferedReader stdError = new BufferedReader(new
				InputStreamReader(p.getErrorStream()));
		try {
			String s = null;
			while ((s = stdError.readLine()) != null) {
				System.out.println(s);
			}
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(stdError);
		}
	}
	
	private String chooseProfiles() {
		return getDeployMode() == DeployMode.LOCAL ? MAVEN_TEST_WORKFLOW_LOCALLY_PROFILE : MAVEN_TEST_WORKFLOW_REMOTELY_PROFILE;
	}
	
	private DeployMode getDeployMode() {
		String deployModeString  = propertiesReader.getProperty(OOZIE_WORKFLOW_DEPLOY_MODE_KEY);
		return DeployMode.valueOf(deployModeString);
	}
	
	private String getOozieServiceLoc() {
		return propertiesReader.getProperty(OOZIE_SERVICE_LOC_KEY);
	}
	
	private String getRunOoozieJobLogFilename() {
		return propertiesReader.getProperty(OOZIE_RUN_WORKFLOW_LOG_FILE_KEY);
	}
	
	private String getNameNode() {
		return propertiesReader.getProperty(NAME_NODE_KEY);
	}
	
	private Status waitForJobFinish(String jobId, long timeoutInSeconds) {

		long timeout = 1000L * timeoutInSeconds;
		long checkInterval = 1000L * 1;
		long startTime = System.currentTimeMillis();
		List<Status> jobFinishedStatuses = Lists.newArrayList(Status.SUCCEEDED, Status.FAILED, Status.KILLED, Status.SUSPENDED);

		while ((System.currentTimeMillis()-startTime)<timeout) {
			Status status;
			try {
				Thread.sleep(checkInterval);
				status = oozieClient.getJobInfo(jobId).getStatus();
			} catch (OozieClientException e) {
				throw new RuntimeException("Unable to check oozie job status", e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

			if (jobFinishedStatuses.contains(status)) {
				return status;
			}

			System.out.println("Job " + jobId + " is still running with status: " + status + " [" + (System.currentTimeMillis()-startTime) + " ms]");
			// log.debug("Job {} is still running with status: {} [{} ms]", new Object[] { jobId, status, System.currentTimeMillis()-startTime });
		}
		
		printOozieJobLog(jobId);
		Assert.fail("Execution of job " + jobId + " exceeded waiting time limit");

		return null;
	}
	
	private void assertJobStatus(String jobId, Status status, Status expectedStatus) {
		if (status != expectedStatus) {
			printOozieJobLog(jobId);
			Assert.fail("Job has finished with status: " + status + " but " + expectedStatus + " was expected");
		}
		
		log.info("Job has finished sucessfully");
	}
	
	private void printOozieJobLog(String jobId) {
		try {
			log.info(oozieClient.getJobLog(jobId));
		} catch (OozieClientException e) {
			log.warn("Unable to check oozie job log");
		}
	}
	
}
