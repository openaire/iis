package eu.dnetlib.iis.core;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

/**
 * Util class for executing commands on remote machines through ssh protocol 
 * 
 * @author madryk
 *
 */
public class SshExecUtils {

	private final static int SSH_EXEC_TIMEOUT_IN_SEC = 5;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	private SshExecUtils() {
		throw new IllegalStateException("Can't instantiate util class " + getClass());
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Executes command on remote machine through ssh.<br/>
	 * Internally uses {@link #sshExec(SSHClient, String, boolean)} with
	 * parameter throwExceptionOnCommandError set to true
	 */
	public static Command sshExec(SSHClient sshClient, String command) throws IOException {
		return sshExec(sshClient, command, true);
	}
	
	/**
	 * Executes command on remote machine through ssh.
	 * 
	 * @param sshClient - client used to execute command
	 * @param command - command that will be executed on remote machine
	 * @param throwExceptionOnCommandError - if true then any error in executing
	 * 		command on remote machine will cause this method to throw exception
	 * @return command execution results
	 * @throws IOException - in case of problems with connection or data transmission
	 * 		between local and remote machines
	 */
	public static Command sshExec(SSHClient sshClient, String command, boolean throwExceptionOnCommandError) throws IOException {
		Session sshSession = null;
		Command cmd = null;
		
		try {
			sshSession = sshClient.startSession();
			
			cmd = sshSession.exec(command);
			
			cmd.join(SSH_EXEC_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
			
			if (throwExceptionOnCommandError && cmd.getExitStatus() != 0) {
				throw new RuntimeException("Error executing command: " + command 
						+ "\n" + IOUtils.readFully(cmd.getErrorStream()).toString());
			}
		} finally {
			if (sshSession != null) {
				sshSession.close();
			}
		}
		
		return cmd;
	}
	
	/**
	 * Reads output of executed ssh command
	 */
	public static String readCommandOutput(Command cmd) throws IOException {
		return IOUtils.readFully(cmd.getInputStream()).toString();
	}
}
