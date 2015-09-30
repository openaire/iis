package eu.dnetlib.iis.core;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.client.*;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.MiniOozieTestCase;
import org.apache.oozie.util.XLog;
import org.junit.After;
import org.junit.Before;


/**
 * A basis to be used as a base class of all test case classes that run 
 * Oozie workflows.
 * <p/>
 * ATTENTION: In order for your test case class that has this class as a parent 
 * to run properly, the Maven project which contains this class has to have
 * a directory called "core" in its root directory with appropriate contents. 
 * See the "core" directory in the "icm-iis-core-examples" to see what's 
 * the required content of this directory. 
 * You can copy this "core" directory to your project.
 * <p/>
 * <p/>
 * If something goes wrong with your test and you want to see the Hadoop logs 
 * to check what's the matter, you can look into directory 
 * <p/>
 * target/test-data/oozietests/NAME_OF_YOUR_TEST_CLASS/NAME_OF_YOUR_TEST_METHOD/userlogs
 * <p/>
 * and its subdirectories. 
 * <p/>
 * Unfortunately, I don't know where the files stored in virtual HDFS really 
 * are (such an information would also be helpful during debugging).
 *  
 * @author Mateusz Kobos
 *
 */
public abstract class AbstractWorkflowTestCase extends MiniOozieTestCase {

	private static final char WORKFLOW_LOCATION_SEPARATOR = '/';
	
	private static final String JOB_PROPERTIES_FILE_NAME = "job.properties";
	
	@Override
	@Before
	protected void setUp() throws Exception {
		System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
		log = new XLog(LogFactory.getLog(getClass()));
		super.setUp();
	}

	@Override
	@After
	protected void tearDown() throws Exception {
		super.tearDown();
	}
   
	/**
	 * See {@link #runWorkflow(String resourcesOozieAppLocalPath, OozieWorkflowTestConfiguration config)}
	 * method for description of parameters
	 */
	public RemoteOozieAppManager runWorkflow(String resourcesOozieAppLocalPath) 
			throws IOException, OozieClientException{
		return runWorkflow(resourcesOozieAppLocalPath, new OozieWorkflowTestConfiguration());
	}
	
	/**
	 * Run the workflow
	 * @param resourcesOozieAppLocalPath path to the directory containing the Oozie
     * application. This directory contains the main {@code workflow.xml} file
     * @param config optional configuration of the workflow
	 */
	public RemoteOozieAppManager runWorkflow(
			String resourcesOozieAppLocalPath, OozieWorkflowTestConfiguration config)
					throws IOException, OozieClientException{
		return runWorkflow(resourcesOozieAppLocalPath, config, false);
	}
	
	/**
	 * Run the workflow
	 * @param resourcesOozieAppLocalPath path to the directory containing the Oozie
     * application. This directory contains the main {@code workflow.xml} file
     * @param config optional configuration of the workflow
     * @param skipPredefinedJobProperties skips reading predefined job.properties if any set
	 */
	public RemoteOozieAppManager runWorkflow(
			String resourcesOozieAppLocalPath, OozieWorkflowTestConfiguration config,
			boolean skipPredefinedJobProperties)
					throws IOException, OozieClientException{
//		File oozieAppLocalPath = 
//		new File(Thread.currentThread().getContextClassLoader()
//				.getResource(resourcesOozieAppLocalPath).getPath());
		RemoteOozieAppManager appManager = RemoteOozieAppManager.fromPrimedClassDir(
			getFileSystem(), getFsTestCaseDir(), resourcesOozieAppLocalPath);
		Properties props = createWorkflowConfiguration(
				createJobConf(), getTestUser(),	appManager.getOozieAppPath(), 
				appManager.getWorkingDir(), 
				skipPredefinedJobProperties?null:
						loadPredefinedJobProperties(resourcesOozieAppLocalPath),
				config.getJobProps());
		runWorkflowBasedOnConfiguration(props, config.getTimeoutInSeconds(),
				config.getExpectedFinishStatus());
		return appManager;
		
	}
	
	/**
	 * Loads job properties defined along with the workflow in job.properties file.
	 * @param resourcesOozieAppLocalPath
	 * @return properties read from predefined job.properties file, null when file not found
	 */
	private Properties loadPredefinedJobProperties(String resourcesOozieAppLocalPath) {
		if (resourcesOozieAppLocalPath!=null && 
				resourcesOozieAppLocalPath.contains(""+WORKFLOW_LOCATION_SEPARATOR)) {
			try {
				String parentWorkflowDir = resourcesOozieAppLocalPath.substring(0, 
						resourcesOozieAppLocalPath.lastIndexOf(WORKFLOW_LOCATION_SEPARATOR));
				Properties resultProperties = new Properties();
				resultProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(
						parentWorkflowDir + WORKFLOW_LOCATION_SEPARATOR + JOB_PROPERTIES_FILE_NAME));
				return resultProperties;
			} catch (Exception e) {
				log.warn("unable to load job.properties for workflow location " + 
						resourcesOozieAppLocalPath);
			}
		}
//		fallback
		return null;
	}
	
	/**
	 * Run the workflow
	 * @deprecated Replaced by {@link #runWorkflow(String)}
	 */
	@Deprecated
	public void runWorkflow(Path oozieAppPath, Path workingDir) 
					throws OozieClientException, IOException{
		runWorkflow(oozieAppPath, workingDir, 
				OozieWorkflowTestConfiguration.defaultJobProperties);
	}
	
	/**
	 * Run the workflow
	 * @deprecated Replaced by {@link #runWorkflow(String, OozieWorkflowTestConfiguration)}
	 */
	@Deprecated
	public void runWorkflow(Path oozieAppPath, Path workingDir,
			Properties jobProperties) 
					throws OozieClientException, IOException{
		OozieWorkflowTestConfiguration config = 
				new OozieWorkflowTestConfiguration().setJobProps(jobProperties);
		Properties props = createWorkflowConfiguration(
				createJobConf(), getTestUser(),	oozieAppPath, 
				workingDir, null, config.getJobProps());
		runWorkflowBasedOnConfiguration(props, config.getTimeoutInSeconds(),
				config.getExpectedFinishStatus());
	}
    
	private static Properties createWorkflowConfiguration(
			JobConf jobConf, String testUser, 
			Path oozieAppPath, Path workingDir, 
			Properties defaultJobProps, Properties runtimeJobProps) 
					throws IOException{		
		OozieTestsIOUtils.saveConfiguration(jobConf);
		
		final OozieClient wc = LocalOozie.getClient();

		Properties conf = wc.createConfiguration();
		/** Standard settings*/
		conf.setProperty(OozieClient.APP_PATH, oozieAppPath.toString());
		conf.setProperty(OozieClient.USER_NAME, testUser);
        conf.setProperty("jobTracker", jobConf.get("mapred.job.tracker"));
        conf.setProperty("nameNode", jobConf.get("fs.default.name"));
		conf.setProperty("queueName", "default");
		/** Custom settings */
		conf.setProperty("workingDir", workingDir.toString());
        conf.setProperty("minioozieTestRun", "true");
		if (defaultJobProps!=null) {
			conf.putAll(defaultJobProps);
		}
		if (runtimeJobProps!=null) {
			conf.putAll(runtimeJobProps);
		}
		return conf;
	}
	
	/**
	 * Execute workflow and checks if it ran correctly.
	 * @param configuration
     * @param timeoutInSeconds
	 * @throws OozieClientException
	 */
	private void runWorkflowBasedOnConfiguration(Properties configuration, 
			int timeoutInSeconds, WorkflowJob.Status expectedFinishStatus)
			throws OozieClientException{
		final OozieClient wc = LocalOozie.getClient();
		final String jobId = wc.submit(configuration);
		assertNotNull(jobId);
		WorkflowJob wf = wc.getJobInfo(jobId);
		assertNotNull(wf);
		assertEquals(WorkflowJob.Status.PREP, wf.getStatus());
		wc.start(jobId);

		waitFor(timeoutInSeconds * 1000, new Predicate() {
			public boolean evaluate() throws Exception {
				WorkflowJob wf = wc.getJobInfo(jobId);
				return (wf.getEndTime() != null);
			}
		});

    	wf = wc.getJobInfo(jobId);
 
        printErrors(wc, wf);
       
		assertNotNull(wf);
		assertEquals(expectedFinishStatus, wf.getStatus());
	}
    
    private void printErrors(OozieClient wc, WorkflowJob wf) {
        /** 
         * We can safely ignore the exceptions thrown during 
         * printing error messages 
         */
        try {
            for (WorkflowAction wa : wf.getActions()) {
                String errorMessage = wa.getErrorMessage() == null ? "" : wa.getErrorMessage();
                log.info("Workflow action {0} {1}", wa.getId(), errorMessage);
                if ("sub-workflow".equals(wa.getType()) && wa.getExternalId() != null) {
                    WorkflowJob subwf = wc.getJobInfo(wa.getExternalId());
                    printErrors(wc, subwf);
                }
            }
        } catch (OozieClientException ex) {}
    }
}
