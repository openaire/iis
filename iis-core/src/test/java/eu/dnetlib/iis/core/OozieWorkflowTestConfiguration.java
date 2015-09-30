package eu.dnetlib.iis.core;

import java.util.Properties;

import org.apache.oozie.client.WorkflowJob;

/**
 * Configuration of workflow test.
 * It contains additional criteria that must met for test to pass.
 * 
 * TODO: clean unused properties after removing mini oozie from project 
 * 
 * @author Mateusz Kobos
 * @author madryk
 */
public class OozieWorkflowTestConfiguration {
	
	/** I had to change the duration of the wait from the original 
	 * 15 seconds because the original time wasn't enough for my 
	 * computer to execute the Oozie workflow. To be more precise, 
	 * the test ended with the following failure message: 
	 * junit.framework.AssertionFailedError: expected:<SUCCEEDED> but was:<RUNNING>
	 */
	public static final int defaultTimeoutInSeconds = 360;
	public static final WorkflowJob.Status defaultExpectedFinishStatus = 
			WorkflowJob.Status.SUCCEEDED;
	public static final Properties defaultJobProperties = null;
	
	private Properties jobProps = defaultJobProperties; 
	private int timeoutInSeconds = defaultTimeoutInSeconds;
	private WorkflowJob.Status expectedFinishStatus = defaultExpectedFinishStatus;
	
	public OozieWorkflowTestConfiguration(){
	}

	/**
	 * See {@link #setJobProps} for description
	 * @return
	 */
	public Properties getJobProps() {
		return jobProps;
	}
	/**
	 * @param jobProps job properties
	 */
	public OozieWorkflowTestConfiguration setJobProps(Properties jobProps) {
		this.jobProps = jobProps;
		return this;
	}
	
	/**
	 * See {@link #setTimeoutInSeconds} for description
	 */
	public int getTimeoutInSeconds() {
		return timeoutInSeconds;
	}
	/**
	 * @param timeoutInSeconds timeout in seconds. Workflow will be killed
     * if timeout is exceeded
	 */
	public OozieWorkflowTestConfiguration setTimeoutInSeconds(int timeoutInSeconds) {
		this.timeoutInSeconds = timeoutInSeconds;
		return this;
	}
	
	/**
	 * See {@link #setExpectedFinishStatus} for description
	 * @return
	 */
	public WorkflowJob.Status getExpectedFinishStatus() {
		return expectedFinishStatus;
	}
	/**
	 * @param expectedFinishStatus expected status of the workflow after its
	 * finish
	 */
	public OozieWorkflowTestConfiguration setExpectedFinishStatus(WorkflowJob.Status expectedFinishStatus) {
		this.expectedFinishStatus = expectedFinishStatus;
		return this;
	}
}
