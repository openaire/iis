package eu.dnetlib.iis.core;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

/**
 * Class used for executing commands on remote machine through ssh protocol.
 * 
 * @author madryk
 *
 */
public class SshSimpleConnection {
	
	private final static int SSH_EXEC_TIMEOUT_IN_SEC = 5;
	
	
	private SSHClient sshClient = new SSHClient();
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Executes command on remote machine through ssh.<br/>
	 * Internally uses {@link #execute(String, boolean)} with
	 * parameter throwExceptionOnCommandError set to true
	 */
	public Command execute(String command) {
		return execute(command, true);
	}
	
	/**
	 * Executes command on remote machine through ssh.
	 * 
	 * @param command - command that will be executed on remote machine
	 * @param throwExceptionOnCommandError - if true then any error in executing
	 * 		command on remote machine will cause this method to throw exception
	 * @return command execution results
	 */
	public Command execute(String command, boolean throwExceptionOnCommandError) {
		Session sshSession = null;
		Command cmd = null;
		
		try {
			sshSession = sshClient.startSession();
			
			cmd = sshSession.exec(command);
			
			cmd.join(SSH_EXEC_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
			
			if (throwExceptionOnCommandError && cmd.getExitStatus() != 0) {
				throw new RuntimeException("Error executing command: " + command 
						+ "\n" + SshExecUtils.readCommandError(cmd));
			}
		} catch (IOException e) {
			throw new RuntimeException("Error in communication with remote machine", e);
		} finally {
			if (sshSession != null) {
				try {
					sshSession.close();
				} catch (IOException e) {
					throw new RuntimeException("Error in closing ssh session", e);
				}
			}
		}
		
		return cmd;
	}
	
	/**
	 * Downloads file (or directory) from remote to local machine
	 * 
	 * @param remotePath - path on remote machine to file to download
	 * @param localPath - path on local machine where file should be downloaded
	 */
	public void download(String remotePath, String localPath) {
		try {
			sshClient.newSCPFileTransfer().download(remotePath, localPath);
		} catch (IOException e) {
			throw new RuntimeException("Error in downloading file", e);
		}
	}
	
	
	//------------------------ PACKAGE-PRIVATE --------------------------
	
	/**
	 * Opens ssh connection.
	 * Method must be executed before any attempt to read from remote host.
	 */
	void openConnection(String remoteHost, int sshPort, String remoteUser) {
		try {
			sshClient.addHostKeyVerifier(new PromiscuousVerifier());
			sshClient.connect(remoteHost, sshPort);
			sshClient.authPublickey(remoteUser);
		} catch (IOException e) {
			throw new RuntimeException("Error in opening ssh connection", e);
		}
	}
	
	/**
	 * Closes ssh connection
	 * After executing this method any attempt to read from remote host
	 * will fail until connection will be open again (see {@link #openConnection()}).
	 */
	void closeConnection() {
		try {
			sshClient.close();
		} catch (IOException e) {
			throw new RuntimeException("Error in closing ssh connection", e);
		}
	}
	
}
