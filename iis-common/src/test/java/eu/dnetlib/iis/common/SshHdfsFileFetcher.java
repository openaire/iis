package eu.dnetlib.iis.common;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Service for fetching files from hdfs using ssh protocol
 * 
 * @author madryk
 *
 */
public class SshHdfsFileFetcher {
	
	public final static String FILE_PATH_SEPARATOR = "/";
	
	
	private final SshConnectionManager sshConnectionManager;
	
	private final String remoteUserDir;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public SshHdfsFileFetcher(SshConnectionManager sshConnectionManager, String remoteTempDir) {
		this.remoteUserDir = appendFilePathSeparatorIfMissing(remoteTempDir);
		this.sshConnectionManager = sshConnectionManager;
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Fetches file (or directory) from hdfs into target directory.
	 * 
	 * @return location of fetched file
	 */
	public File fetchFile(String hdfsPath, File targetDir) throws FileNotFoundException {
		
		String filename = new File(hdfsPath).getName();
		File localTargetFile = new File(targetDir, filename);
		String remoteFileTempDir = remoteUserDir + "temp_copy_" + System.currentTimeMillis() + FILE_PATH_SEPARATOR;


		checkIfFileExistsOnHdfs(hdfsPath);

		makeDirOnRemote(remoteFileTempDir);

		copyFromHdfsOnRemote(hdfsPath, remoteFileTempDir);

		downloadFromRemote(remoteFileTempDir + filename, localTargetFile);

		removeFromRemote(remoteFileTempDir);
		
		return localTargetFile;
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private void checkIfFileExistsOnHdfs(String hdfsPath) throws FileNotFoundException {
		SshSimpleConnection sshConnection = sshConnectionManager.getConnection();
		
		if (sshConnection.execute("hadoop fs -test -e " + hdfsPath, false).getExitStatus() != 0) {
			throw new FileNotFoundException("File " + hdfsPath + " not found on hdfs");
		}
	}
	
	private void makeDirOnRemote(String hdfsPath) {
		sshConnectionManager.getConnection().execute("mkdir -p " + hdfsPath);
	}
	
	private void copyFromHdfsOnRemote(String hdfsSource, String remoteTarget) {
		sshConnectionManager.getConnection().execute("hadoop fs -get " + hdfsSource + " " + remoteTarget);
	}
	
	private void downloadFromRemote(String remoteSource, File localTarget) {
		
		localTarget.getParentFile().mkdirs();
		
		sshConnectionManager.getConnection().download(remoteSource, localTarget.getAbsolutePath());
	}
	
	private void removeFromRemote(String remotePath) {
		sshConnectionManager.getConnection().execute("rm -r " + remotePath);
	}
	
	
	private String appendFilePathSeparatorIfMissing(String directoryPath) {
		return directoryPath + (directoryPath.endsWith(FILE_PATH_SEPARATOR) ? "" : FILE_PATH_SEPARATOR);
	}
	
}
