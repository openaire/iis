package eu.dnetlib.iis.core;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import eu.dnetlib.iis.IntegrationTest;

/**
 * Base class for testing oozie workflows on cluster
 * 
 * @author madryk
 *
 */
@Category(IntegrationTest.class)
public abstract class AbstractOozieWorkflowTestCase {
	
	private final static Logger log = LoggerFactory.getLogger(AbstractOozieWorkflowTestCase.class);
	
	
	private final static String OOZIE_SERVICE_LOC_KEY = "oozieServiceLoc";
	
	private final static String OOZIE_RUN_WORKFLOW_LOG_FILE_KEY = "oozie.execution.log.file.location";
	
	private final static String REMOTE_HOST_NAME_KEY = "iis.hadoop.frontend.host.name";
	
	private final static String REMOTE_USER_NAME_KEY = "iis.hadoop.frontend.user.name";
	
	private final static String REMOTE_TEMP_DIR_KEY = "iis.hadoop.frontend.temp.dir";
	
	private final static String REMOTE_SSH_PORT_KEY = "iis.hadoop.frontend.port.ssh";
	
	private final static String MAVEN_EXECUTABLE = "maven.executable";
	
	
	private static Properties properties;
	
	private static File propertiesFile;
	
	private MavenTestWorkflowRunner mvnTestWorkflowRunner;
	
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
		
	    mvnTestWorkflowRunner = new MavenTestWorkflowRunner(getMavenExecutable());
	    
		log.debug("Setting up OozieClient at {}", getOozieServiceLoc());
		sshConnectionManager = new SshConnectionManager(getRemoteHostName(), getRemoteSshPort(), getRemoteUserName());
		sshOozieClient = new SshOozieClient(sshConnectionManager, getOozieServiceLoc());
		
		SshHdfsFileFetcher hdfsFileFetcher = new SshHdfsFileFetcher(sshConnectionManager, getRemoteTempDir());
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

	    int exitStatus = mvnTestWorkflowRunner.runTestWorkflow(workflowPath, propertiesFile.getAbsolutePath());
	    if (exitStatus != 0) {
	        Assert.fail("Maven run workflow process failed");
	    }
		
		
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
	
	private String getRemoteTempDir() {
		return getProperty(REMOTE_TEMP_DIR_KEY);
	}
	
	private int getRemoteSshPort() {
		return Integer.valueOf(getProperty(REMOTE_SSH_PORT_KEY));
	}
	
	private String getMavenExecutable() {
	    return getProperty(MAVEN_EXECUTABLE);
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

			log.debug("Job {} is still running with status: {} [{} ms]", new Object[] { jobId, status, System.currentTimeMillis()-startTime });
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
