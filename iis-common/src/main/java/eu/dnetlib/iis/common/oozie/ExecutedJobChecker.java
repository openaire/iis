package eu.dnetlib.iis.common.oozie;

import java.security.InvalidParameterException;
import java.util.List;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

/**
 * Oozie client executed job checker.
 * Throws exception when given job is already running.
 * 
 * expected params:
 * args[0] - oozie service location
 * args[1] - workflow name
 * args[2] - current workflow id, to be excluded from potential hits
 * 
 * @author mhorst
 *
 */
public class ExecutedJobChecker {

	public static void main(String[] args) throws Exception {
		if (args.length==3) {
			OozieClient client = new OozieClient(args[0]);
			StringBuffer filter = new StringBuffer();
			filter.append(OozieClient.FILTER_NAME);
			filter.append('=');
			filter.append(args[1]);
			filter.append(';');
			filter.append(OozieClient.FILTER_STATUS);
			filter.append('=');
			filter.append("RUNNING");
			List<WorkflowJob> wfJobs = client.getJobsInfo(filter.toString());
			if (wfJobs.size()>0) {
				for (WorkflowJob currentJob : wfJobs) {
					if (!args[2].equals(currentJob.getId())) {
						throw new RuntimeException("job " + args[1] + 
								" is currently running! Job details: " + 
								generateJobDetails(currentJob));		
					}
				}
			}
		} else {
			throw new InvalidParameterException("invalid number arguments: " + 
					args.length + ", expected: 3");
		}
	}
	
	private static String generateJobDetails(WorkflowJob job) {
		StringBuffer strBuff = new StringBuffer();
		strBuff.append("jobId: ");
		strBuff.append(job.getId());
		strBuff.append(", started at: ");
		strBuff.append(job.getStartTime());
		strBuff.append(", last modified at: ");
		strBuff.append(job.getLastModifiedTime());
		return strBuff.toString();
	}
	
}
