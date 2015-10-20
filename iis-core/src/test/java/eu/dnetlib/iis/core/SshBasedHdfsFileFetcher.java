package eu.dnetlib.iis.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import net.schmizz.sshj.SSHClient;

/**
 * Service for fetching files from hdfs using ssh protocol
 * 
 * @author madryk
 *
 */
public class SshBasedHdfsFileFetcher {
	
	public final static String FILE_PATH_SEPARATOR = "/";
	
	
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
		if (SshExecUtils.sshExec(sshClient, "hadoop fs -test -e " + hdfsPath, false).getExitStatus() != 0) {
			throw new FileNotFoundException("File " + hdfsPath + " not found on hdfs");
		}
	}
	
	private void makeDirOnRemote(SSHClient sshClient, String hdfsPath) throws IOException {
		SshExecUtils.sshExec(sshClient, "mkdir -p " + hdfsPath);
	}
	
	private void copyFromHdfsOnRemote(SSHClient sshClient, String hdfsSource, String remoteTarget) throws IOException {
		SshExecUtils.sshExec(sshClient, "hadoop fs -get " + hdfsSource + " " + remoteTarget);
	}
	
	private void downloadFromRemote(SSHClient sshClient, String remoteSource, File localTarget) throws IOException {
		
		localTarget.getParentFile().mkdirs();
		
		sshClient.newSCPFileTransfer().download(remoteSource, localTarget.getAbsolutePath());
	}
	
	private void removeFromRemote(SSHClient sshClient, String remotePath) throws IOException {
		SshExecUtils.sshExec(sshClient, "rm -r " + remotePath);
	}
	
	
	private String appendFilePathSeparatorIfMissing(String directoryPath) {
		return directoryPath + (directoryPath.endsWith(FILE_PATH_SEPARATOR) ? "" : FILE_PATH_SEPARATOR);
	}
	
}
