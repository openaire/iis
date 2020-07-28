package eu.dnetlib.iis.wf.referenceextraction.patent;

/**
 * Open patent web service connection details.
 * 
 * @author mhorst
 *
 */
public class ConnectionDetails {
    
    private final int connectionTimeout;
    private final int readTimeout;
    private final String authHostName;
    private final int authPort;
    private final String authScheme;
    private final String authUriRoot;
    private final String opsHostName; 
    private final int opsPort;
    private final String opsScheme; 
    private final String opsUriRoot; 
    private final String consumerCredential;
    private final long throttleSleepTime;
    private final int maxRetriesCount;
    
    ConnectionDetails(int connectionTimeout, int readTimeout, 
            String authHostName, int authPort, String authScheme, String authUriRoot,
            String opsHostName, int opsPort, String opsScheme, String opsUriRoot, 
            String consumerCredential, long throttleSleepTime, int maxRetriesCount) {
       this.connectionTimeout = connectionTimeout;
       this.readTimeout = readTimeout;
       
       this.authHostName = authHostName;
       this.authPort = authPort;
       this.authScheme = authScheme;
       this.authUriRoot = authUriRoot;
       
       this.opsHostName = opsHostName;
       this.opsPort = opsPort;
       this.opsScheme = opsScheme;
       this.opsUriRoot = opsUriRoot;
       
       this.consumerCredential = consumerCredential;
       this.throttleSleepTime = throttleSleepTime;
       this.maxRetriesCount = maxRetriesCount;
    }
    
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public String getAuthHostName() {
        return authHostName;
    }

    public int getAuthPort() {
        return authPort;
    }

    public String getAuthScheme() {
        return authScheme;
    }

    public String getAuthUriRoot() {
        return authUriRoot;
    }

    public String getOpsHostName() {
        return opsHostName;
    }

    public int getOpsPort() {
        return opsPort;
    }

    public String getOpsScheme() {
        return opsScheme;
    }

    public String getOpsUriRoot() {
        return opsUriRoot;
    }

    public String getConsumerCredential() {
        return consumerCredential;
    }

    public long getThrottleSleepTime() {
        return throttleSleepTime;
    }

    public int getMaxRetriesCount() {
        return maxRetriesCount;
    }

}