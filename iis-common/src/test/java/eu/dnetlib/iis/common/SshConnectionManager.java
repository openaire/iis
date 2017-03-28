package eu.dnetlib.iis.common;

/**
 * Manager of ssh connection
 * 
 * @author madryk
 *
 */
public class SshConnectionManager {

	private final String remoteHost;
	
	private final int sshPort;
	
	private final String remoteUser;
	
	
	private SshSimpleConnection sshSimpleConnection;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * Default constructor
	 * 
	 * @param remoteHost - remote machine address
	 * @param remoteUser - remote machine user
	 */
	public SshConnectionManager(String remoteHost, int sshPort, String remoteUser) {
		this.remoteHost = remoteHost;
		this.sshPort = sshPort;
		this.remoteUser = remoteUser;
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Returns currently managed ssh connection.
	 * If it does not exist then it will be created.
	 */
	public SshSimpleConnection getConnection() {
		if (sshSimpleConnection != null) {
			return sshSimpleConnection;
		}
		
		sshSimpleConnection = new SshSimpleConnection();
		
		sshSimpleConnection.openConnection(remoteHost, sshPort, remoteUser);
		
		return sshSimpleConnection;
	}
	
	/**
	 * Closes currently managed ssh connection.
	 * Nothing happens if there is no managed connection.
	 */
	public void closeConnection() {
		if (sshSimpleConnection != null) {
			sshSimpleConnection.closeConnection();
			sshSimpleConnection = null;
		}
	}
}
