package eu.dnetlib.iis.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;

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
		
		
		String s = "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>"
				+ "<configuration>"
				+ "<property><name>fs.defaultFS</name><value>" + getNameNode() + "</value></property>"
				+ "</configuration>";
		
		
		Configuration hdfsConf = new Configuration(false);
		hdfsConf.addResource(new ReaderInputStream(new StringReader(s)));
		
		hadoopFilesystem = FileSystem.get(hdfsConf);
		
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
		
		
		String jobId = getJobId();
		
		
		Status jobStatus = waitForJobFinish(jobId, configuration.getTimeoutInSeconds());
		
		assertJobStatus(jobId, jobStatus, configuration.getExpectedFinishStatus());
		
		
		Properties jobProperties = fetchJobProperties(jobId);
		String workflowWorkingDir = jobProperties.getProperty("workingDir");
		
		WorkflowTestResult result = new WorkflowTestResult();
		
		importFilesFromHdfs(result, configuration.getOutputFilesToInclude(), workflowWorkingDir);
		importAvroDatastoresFromHdfs(result, configuration.getOutputAvroDataStoreToInclude(), workflowWorkingDir);
		
		return result;
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private Properties fetchJobProperties(String jobId) {
		Properties jobProperties = new Properties();
		
		try {
			WorkflowJob jobInfo = oozieClient.getJobInfo(jobId);
			String jobConfigurationString = jobInfo.getConf();
			
			SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(new StringReader(jobConfigurationString));
			
			for (Object o : doc.getRootElement().getChildren()) {
				Element propertyElement = (Element) o;
				String propertyName = propertyElement.getChildText("name");
				String propertyValue = propertyElement.getChildText("value");
				
				jobProperties.put(propertyName, propertyValue);
				
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return jobProperties;
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
	
	private String getNameNode() {
		return propertiesReader.getProperty(NAME_NODE_KEY);
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
	
	private void importFilesFromHdfs(WorkflowTestResult result, List<String> filesPaths, String workingDir) {

		for (String filePath : filesPaths) {
			String tempFilename = new File(filePath).getName();

			try {
				hadoopFilesystem.copyToLocalFile(new Path(workingDir, filePath), new Path(tempDir.getAbsolutePath(), tempFilename));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			result.addWorkflowOutputFiles(filePath, new File(tempDir, tempFilename));
		}
	}
	
	private void importAvroDatastoresFromHdfs(WorkflowTestResult result, List<String> datastoresPaths, String workingDir) {
		
		for (String requestedDataStore : datastoresPaths) {
			List<SpecificRecord> dataStore = null;
			
			try {
				dataStore = DataStore.read(new FileSystemPath(hadoopFilesystem, new Path(workingDir, requestedDataStore)));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			result.addAvroDataStore(requestedDataStore, dataStore);
		}
	}
	
}
