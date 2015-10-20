package eu.dnetlib.iis.core;

import java.io.IOException;
import java.util.Properties;

import org.apache.oozie.client.WorkflowJob.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import net.schmizz.sshj.SSHClient;

/**
 * Service for retrieving informations about jobs on running oozie instance.
 * It communicates indirectly with oozie through some remote host using ssh protocol.
 * 
 * @author madryk
 *
 */
public class SshOozieClient {
	
	private Logger log = LoggerFactory.getLogger(getClass());
	
	
	private OozieCmdLineAnswerParser oozieCmdLineParser = new OozieCmdLineAnswerParser();
	
	
	private String remoteHost;
	
	private String remoteUser;
	
	private String oozieUrl;
	
	
	private SSHClient sshClient;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * Default constructor
	 * 
	 * @param remoteHost - address of host that will be responsible for communication with oozie
	 * @param remoteUser - name of user on host machine
	 * @param oozieUrl - address of oozie (from remote host viewpoint) 
	 */
	public SshOozieClient(String remoteHost, String remoteUser, String oozieUrl) {
		this.remoteHost = remoteHost;
		this.remoteUser = remoteUser;
		this.oozieUrl = oozieUrl;
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Opens ssh connection.<br/>
	 * Method must be executed before any attempt to read from remote host.
	 */
	public void openConnection() throws IOException {
		if (sshClient != null) {
			log.warn("Attempt to open new connection when the old one is still opened. Will use old connection.");
			return;
		}
		
		sshClient = new SSHClient();

		sshClient.loadKnownHosts();
		sshClient.connect(remoteHost);
		sshClient.authPublickey(remoteUser);
	}
	
	/**
	 * Closes ssh connection.<br/>
	 * After executing this method any attempt to read from remote host
	 * will fail until connection will be open again (see {@link #openConnection()}).
	 */
	public void closeConnection() throws IOException {
		if (sshClient != null) {
			sshClient.close();
			sshClient = null;
		}
	}
	
	/**
	 * Returns job status with provided id
	 */
	public Status getJobStatus(String jobId) throws IOException {
		Preconditions.checkArgument(isConnectionOpen(), "Connection must be opened first");
		
		String jobInfoString = SshExecUtils.readCommandOutput(
				SshExecUtils.sshExec(sshClient, buildOozieJobCommand(jobId, "info")));
		
		return oozieCmdLineParser.readStatusFromJobInfo(jobInfoString);
	}
	
	/**
	 * Returns log of job with provided id
	 */
	public String getJobLog(String jobId) throws IOException {
		Preconditions.checkArgument(isConnectionOpen(), "Connection must be opened first");
		
		String jobLog = SshExecUtils.readCommandOutput(
				SshExecUtils.sshExec(sshClient, buildOozieJobCommand(jobId, "log")));
		
		return jobLog;
	}
	
	/**
	 * Returns properties of job with provided id
	 */
	public Properties getJobProperties(String jobId) throws IOException {
		Preconditions.checkArgument(isConnectionOpen(), "Connection must be opened first");
		
		String jobPropertiesString = SshExecUtils.readCommandOutput(
				SshExecUtils.sshExec(sshClient, buildOozieJobCommand(jobId, "configcontent")));
		
		return oozieCmdLineParser.parseJobProperties(jobPropertiesString); 
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private String buildOozieJobCommand(String jobId, String commandName) {
		return "oozie job -oozie " + oozieUrl + " -" + commandName + " " + jobId;
	}
	
	private boolean isConnectionOpen() {
		return sshClient != null;
	}
	
}
