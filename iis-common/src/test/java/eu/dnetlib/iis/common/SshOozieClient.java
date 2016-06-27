package eu.dnetlib.iis.common;

import java.util.Properties;

import org.apache.oozie.client.WorkflowJob.Status;

import net.schmizz.sshj.connection.channel.direct.Session.Command;

/**
 * Service for retrieving informations about jobs on running oozie instance.
 * It communicates indirectly with oozie through some remote host using ssh protocol.
 * 
 * @author madryk
 *
 */
public class SshOozieClient {
	
	
	private SshConnectionManager sshConnectionManager;
	
	private OozieCmdLineAnswerParser oozieCmdLineParser = new OozieCmdLineAnswerParser();
	
	
	private String oozieUrl;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * Default constructor
	 * 
	 * @param sshConnectionManager
	 * @param oozieUrl - address of oozie (from remote host viewpoint) 
	 */
	public SshOozieClient(SshConnectionManager sshConnectionManager, String oozieUrl) {
		this.oozieUrl = oozieUrl;
		this.sshConnectionManager = sshConnectionManager;
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Returns job status with provided id
	 */
	public Status getJobStatus(String jobId) {
                String jobInfoString = executeOozieJobCommand("info", jobId);
		return oozieCmdLineParser.readStatusFromJobInfo(jobInfoString);
	}
	
	/**
	 * Returns log of job with provided id
	 */
	public String getJobLog(String jobId) {
	    return executeOozieJobCommand("log", jobId);
	}
	
	/**
	 * Returns properties of job with provided id
	 */
	public Properties getJobProperties(String jobId) {
                String jobPropertiesString = executeOozieJobCommand("configcontent", jobId);
		return oozieCmdLineParser.parseJobProperties(jobPropertiesString); 
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private String buildOozieJobCommand(String jobId, String commandName) {
		return "oozie job -oozie " + oozieUrl + " -" + commandName + " " + jobId;
	}
	
    private String executeOozieJobCommand(String commandName, String jobId) {
        SshSimpleConnection sshConnection = sshConnectionManager.getConnection();

        Command execResults = sshConnection.execute(buildOozieJobCommand(jobId, commandName));

        return SshExecUtils.readCommandOutput(execResults);
    }
}
