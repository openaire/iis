package eu.dnetlib.iis.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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
	
	private final static String WORKFLOW_SOURCE_DIR_KEY = "workflow.source.dir";
	
	private final static String MAVEN_TEST_WORKFLOW_PHASE = "clean package";
	
	private final static String MAVEN_TEST_WORKFLOW_LOCALLY_PROFILE = "attach-test-resources,oozie-package,deploy-local,run-local";
	
	private final static String MAVEN_TEST_WORKFLOW_REMOTELY_PROFILE = "attach-test-resources,oozie-package,deploy,run";
	

	private static IntegrationTestPropertiesReader propertiesReader;
	
	private OozieClient oozieClient;
	
	
	private enum DeployMode {
		LOCAL,
		SSH
	}
	
	
	@BeforeClass
	public static void classSetUp() {
		propertiesReader = new IntegrationTestPropertiesReader();
	}
	
	@Before
	public void setUp() {
		
		log.debug("Setting up OozieClient at {}", getOozieServiceLoc());
		oozieClient = new OozieClient(getOozieServiceLoc());
	}
	
	/**
	 * Tests workflow located under {@literal workflowLocation} parameter.
	 * Internally uses {@link #testWorkflow(String, OozieWorkflowTestConfiguration)}
	 * with default {@link OozieWorkflowTestConfiguration}
	 */
	protected void testWorkflow(String workflowLocation) {
		testWorkflow(workflowLocation, new OozieWorkflowTestConfiguration());
	}
	
	/**
	 * Tests workflow located under {@literal workflowLocation} parameter.
	 * 
	 * @param workflowLocation - Folder that contains workflow data.
	 *  	It must contain file {@literal oozie_app/workflow.xml} with workflow definition.
	 * @param configuration - some additional test pass criteria
	 *  	(for example timeout - workflow must finish its execution before specified amount of time, 
	 *   	otherwise test will automatically fail)
	 */
	protected void testWorkflow(String workflowLocation, OozieWorkflowTestConfiguration configuration) {
		
		Process p = runMavenTestWorkflow(workflowLocation);

		logMavenOutput(p);
		
		
		String jobId = getJobId();
		
		Status jobStatus = waitForJobFinish(jobId, configuration.getTimeoutInSeconds());
		
		assertJobStatus(jobId, jobStatus);
	}
	
	
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
	
	private String getJobId() {
		
		String jobId;
		try {
			jobId = FileUtils.readFileToString(new File(getRunOoozieJobLogFilename()));
		} catch (IOException e) {
			throw new RuntimeException("Unable to read run oozie job log file", e);
		}
		Pattern pattern = Pattern.compile("^job: (\\S*)$", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(jobId);
		matcher.find();
		jobId = matcher.group(1);
		
		return jobId;
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
//    		log.debug("Job {} is still running with status: {} [{} ms]", new Object[] { jobId, status, System.currentTimeMillis()-startTime });
    	}
    	try {
			log.info(oozieClient.getJobLog(jobId));
		} catch (OozieClientException e) {
			log.warn("Unable to check job log");
		}
    	Assert.fail("Execution of job " + jobId + " exceeded waiting time limit");
    	
    	return null;
	}
	
	private void assertJobStatus(String jobId, Status status) {
		if (status == Status.FAILED || status == Status.KILLED || status == Status.SUSPENDED) {
			
			try {
				log.error(oozieClient.getJobLog(jobId));
			} catch (OozieClientException e) {
				log.warn("Unable to check oozie job log");
			}
			Assert.fail("Job has finished with status: " + status);
		}
		
		log.info("Job has finished sucessfully");
	}
}
