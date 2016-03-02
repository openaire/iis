package eu.dnetlib.maven.plugin.oozie;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;



/**
 * Checks oozie job status.
 * @author mhorst
 *
 * @goal check-job-status
 */
public class CheckJobStatus extends AbstractMojo {
	

    public static final String PROPERTY_NAME_LOG_FILE_LOCATION = "logFileLocation";
	
	/**
	 * @parameter expression="${properties.oozieLocation}"
	 */
	private String oozieLocation;
	
	/**
	 * @parameter expression="${properties.jobId}"
	 */
	private String jobId;
	
	/**
	 * @parameter expression="${properties.maxExecutionTimeMins}"
	 */
	private Integer maxExecutionTimeMins;
	
	/**
	 * @parameter expression="${properties.checkIntervalSecs}"
	 */
	private Integer checkIntervalSecs = 60;
	
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
    	OozieClient oozieClient = new OozieClient(oozieLocation);
//    	OozieClient oozieClient = new AuthOozieClient(oozieLocation);
    	long maxExecutionTime = 1000L * 60 * maxExecutionTimeMins;
    	long checkInterval = 1000L * checkIntervalSecs;
    	long startTime = System.currentTimeMillis();
    	try {
        	try {
        		while ((System.currentTimeMillis()-startTime)<maxExecutionTime) {
        			Thread.sleep(checkInterval);
        			Status status = oozieClient.getJobInfo(jobId).getStatus();
        			if (Status.SUCCEEDED.equals(status)) {
        				getLog().info("job SUCCEEDED");
        				return;
        			} else if (Status.FAILED.equals(status) || Status.KILLED.equals(status)) {
        				System.out.print(oozieClient.getJobLog(jobId));
        				throw new MojoFailureException("job " + jobId + " finished with status: " + status);
        			} else {
        				getLog().info("job " + jobId + " is still running with status: " + status);
        			}
        		}
        		System.out.print(oozieClient.getJobLog(jobId));
            	throw new MojoFailureException("Maximum execution time has passed!");
            	
        	} catch (InterruptedException e) {
        		System.out.print(oozieClient.getJobLog(jobId));
        		throw new MojoExecutionException("exception occured while sleeping", e);
    		}
    	} catch (OozieClientException e) {
			throw new MojoExecutionException("exception occured when obtaining status from oozie", e);
		}
    }

}
