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
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
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
	
	private final static String REMOTE_HOST_NAME_KEY = "iis.hadoop.frontend.host.name";
	
	private final static String REMOTE_USER_NAME_KEY = "iis.hadoop.frontend.user.name";
	
	private final static String REMOTE_HOME_DIR_KEY = "iis.hadoop.frontend.home.dir";
	
	private final static String REMOTE_USER_DIR_KEY = "iis.hadoop.frontend.user.dir";
	
	private final static String REMOTE_SSH_PORT_KEY = "iis.hadoop.frontend.port.ssh";
	
	private final static String WORKFLOW_SOURCE_DIR_KEY = "workflow.source.dir";
	
	private final static String MAVEN_TEST_WORKFLOW_PHASE = "clean package";
	
	private final static String MAVEN_TEST_WORKFLOW_PROFILE = "attach-test-resources,oozie-package,deploy,run";
	
	
	private static Properties properties;
	
	private static File propertiesFile;
	
	private SshConnectionManager sshConnectionManager;
	
	private SshOozieClient sshOozieClient;
	
	private HdfsTestHelper hdfsTestHelper;
	
	private File tempDir;
	
	
	@BeforeClass
	public static void classSetUp() throws IOException {
		IntegrationTestPropertiesReader propertiesReader = new IntegrationTestPropertiesReader();
		properties = propertiesReader.readProperties();
		
		propertiesFile = File.createTempFile("iis-integration-test", ".properties");
		PropertiesFileUtils.writePropertiesToFile(properties, propertiesFile);
	}
	
	@Before
	public void setUp() throws IOException, OozieClientException {
		
		log.debug("Setting up OozieClient at {}", getOozieServiceLoc());
		sshConnectionManager = new SshConnectionManager(getRemoteHostName(), getRemoteSshPort(), getRemoteUserName());
		sshOozieClient = new SshOozieClient(sshConnectionManager, getOozieServiceLoc());
		
		SshHdfsFileFetcher hdfsFileFetcher = new SshHdfsFileFetcher(sshConnectionManager,
				getRemoteHomeDir() + "/" + getRemoteUserDir());
		hdfsTestHelper = new HdfsTestHelper(hdfsFileFetcher);
		
		tempDir = Files.createTempDir();
	}
	
	@After
	public void cleanup() throws IOException {
		if (tempDir != null) {
			FileUtils.deleteDirectory(tempDir);
		}
		sshConnectionManager.closeConnection();
	}
	
	@AfterClass
	public static void classCleanup() {
		propertiesFile.delete();
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
		checkMavenExitStatus(p);
		
		
		String jobId = OozieLogFileParser.readJobIdFromLogFile(new File(getRunOoozieJobLogFilename()));
		
		
		Status jobStatus = waitForJobFinish(jobId, configuration.getTimeoutInSeconds());
		
		assertJobStatus(jobId, jobStatus, configuration.getExpectedFinishStatus());
		
		
		Properties jobProperties = sshOozieClient.getJobProperties(jobId);
		String workflowWorkingDir = jobProperties.getProperty("workingDir");
		
		WorkflowTestResult result = new WorkflowTestResult();
		
		Map<String, File> workflowOutputFiles = hdfsTestHelper.copyFilesFromHdfs(workflowWorkingDir, configuration.getExpectedOutputFiles(), tempDir);
		result.setWorkflowOutputFiles(workflowOutputFiles);
		
		Map<String, List<? extends SpecificRecord>> workflowOutputDataStores = 
				hdfsTestHelper.readAvroDatastoresFromHdfs( workflowWorkingDir, configuration.getExpectedOutputAvroDataStore());
		result.setWorkflowOutputAvroDataStores(workflowOutputDataStores);
		
		return result;
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private Process runMavenTestWorkflow(String workflowSource) {
		
		Process p;
		try {
			p = Runtime.getRuntime().exec("mvn " + MAVEN_TEST_WORKFLOW_PHASE + " -DskipTests "
					+ " -P" + MAVEN_TEST_WORKFLOW_PROFILE
					+ " -D" + WORKFLOW_SOURCE_DIR_KEY + "=" + workflowSource
					+ " -DiisConnectionProperties=" + propertiesFile.getAbsolutePath()
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
	
	private void checkMavenExitStatus(Process p) {
		int exitStatus;
		try {
			exitStatus = p.waitFor();
		} catch (InterruptedException e) {
			throw new RuntimeException("Error in waiting for maven process to finish", e);
		}
		
		if (exitStatus != 0) {
			Assert.fail("Maven run workflow process failed");
		}
	}
	
	private String getOozieServiceLoc() {
		return getProperty(OOZIE_SERVICE_LOC_KEY);
	}
	
	private String getRunOoozieJobLogFilename() {
		return getProperty(OOZIE_RUN_WORKFLOW_LOG_FILE_KEY);
	}
	
	private String getRemoteHostName() {
		return getProperty(REMOTE_HOST_NAME_KEY);
	}
	
	private String getRemoteUserName() {
		return getProperty(REMOTE_USER_NAME_KEY);
	}
	
	private String getRemoteHomeDir() {
		return getProperty(REMOTE_HOME_DIR_KEY);
	}
	
	private String getRemoteUserDir() {
		return getProperty(REMOTE_USER_DIR_KEY);
	}
	
	private int getRemoteSshPort() {
		return Integer.valueOf(getProperty(REMOTE_SSH_PORT_KEY));
	}
	
	private String getProperty(String key) {
		Preconditions.checkArgument(properties.containsKey(key), "Property '%s' is not defined for integration tests", key);
		return properties.getProperty(key);
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
				status = sshOozieClient.getJobStatus(jobId);
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
		log.info(sshOozieClient.getJobLog(jobId));
	}
	
}
