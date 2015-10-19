package eu.dnetlib.iis.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

/**
 * Service for fetching files from hdfs using ssh protocol
 * 
 * @author madryk
 *
 */
public class SshBasedHdfsFileFetcher {
	
	public final static String FILE_PATH_SEPARATOR = "/";
	
	public final static int SSH_EXEC_TIMEOUT_IN_SEC = 5;
	
	
	private String remoteHost;
	
	private String remoteUser;
	
	private String remoteUserDir;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public SshBasedHdfsFileFetcher(String remoteHost, String remoteUser, String remoteUserDir) {
		this.remoteHost = remoteHost;
		this.remoteUser = remoteUser;
		this.remoteUserDir = appendFilePathSeparatorIfMissing(remoteUserDir);
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Fetches file (or directory) from hdfs into target directory.
	 * 
	 * @return location of fetched file
	 */
	public File fetchFile(String hdfsPath, File targetDir) throws IOException {
		
		String filename = new File(hdfsPath).getName();
		File localTargetFile = new File(targetDir, filename);
		String remoteFileTempDir = remoteUserDir + "temp_copy_" + System.currentTimeMillis() + FILE_PATH_SEPARATOR;
		
		SSHClient sshClient = new SSHClient();
		
		try {
			sshClient.loadKnownHosts();
			sshClient.connect(remoteHost);
			sshClient.authPublickey(remoteUser);

			checkIfFileExistsOnHdfs(sshClient, hdfsPath);

			makeDirOnRemote(sshClient, remoteFileTempDir);

			copyFromHdfsOnRemote(sshClient, hdfsPath, remoteFileTempDir);

			downloadFromRemote(sshClient, remoteFileTempDir + filename, localTargetFile);

			removeFromRemote(sshClient, remoteFileTempDir);

		} finally {
			sshClient.close();
		}
		
		return localTargetFile;
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private void checkIfFileExistsOnHdfs(SSHClient sshClient, String hdfsPath) throws IOException {
		if (sshExec(sshClient, "hadoop fs -test -e " + hdfsPath, false) != 0) {
			throw new FileNotFoundException("File " + hdfsPath + " not found on hdfs");
		}
	}
	
	private void makeDirOnRemote(SSHClient sshClient, String hdfsPath) throws IOException {
		sshExec(sshClient, "mkdir -p " + hdfsPath);
	}
	
	private void copyFromHdfsOnRemote(SSHClient sshClient, String hdfsSource, String remoteTarget) throws IOException {
		sshExec(sshClient, "hadoop fs -get " + hdfsSource + " " + remoteTarget);
	}
	
	private void downloadFromRemote(SSHClient sshClient, String remoteSource, File localTarget) throws IOException {
		
		localTarget.getParentFile().mkdirs();
		
		sshClient.newSCPFileTransfer().download(remoteSource, localTarget.getAbsolutePath());
	}
	
	private void removeFromRemote(SSHClient sshClient, String remotePath) throws IOException {
		sshExec(sshClient, "rm -r " + remotePath);
	}
	
	
	private int sshExec(SSHClient sshClient, String command) throws IOException {
		return sshExec(sshClient, command, true);
	}
	
	private int sshExec(SSHClient sshClient, String command, boolean throwExceptionOnCommandError) throws IOException {
		Session sshSession = null;
		int exitStatus;
		try {
			sshSession = sshClient.startSession();

			Command cmd = sshSession.exec(command);
			
			cmd.join(SSH_EXEC_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
			
			exitStatus = cmd.getExitStatus();
			if (exitStatus != 0 && throwExceptionOnCommandError) {
				throw new RuntimeException("Error executing command: " + command 
						+ "\n" + IOUtils.readFully(cmd.getErrorStream()).toString());
			}
		} finally {
			if (sshSession != null) {
				sshSession.close();
			}
		}
		
		return exitStatus;
	}
	
	private String appendFilePathSeparatorIfMissing(String directoryPath) {
		return directoryPath + (directoryPath.endsWith(FILE_PATH_SEPARATOR) ? "" : FILE_PATH_SEPARATOR);
	}
	
}
