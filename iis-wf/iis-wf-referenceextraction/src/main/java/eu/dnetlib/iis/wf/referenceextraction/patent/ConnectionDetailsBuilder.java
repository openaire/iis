package eu.dnetlib.iis.wf.referenceextraction.patent;

/**
 * Open patent web service connection details builder.
 * 
 * @author mhorst
 *
 */
public class ConnectionDetailsBuilder {
    
    private int connectionTimeout;
    private int readTimeout;
    private String authHostName;
    private int authPort;
    private String authScheme;
    private String authUriRoot;
    private String opsHostName; 
    private int opsPort;
    private String opsScheme; 
    private String opsUriRoot; 
    private String consumerCredential;
    private long throttleSleepTime;
    private int maxRetriesCount;
    
    public static ConnectionDetailsBuilder newBuilder() {
        return new ConnectionDetailsBuilder();
    }
    
    public ConnectionDetailsBuilder withConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }
    
    public ConnectionDetailsBuilder withReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }
    
    public ConnectionDetailsBuilder withAuthHostName(String authHostName) {
        this.authHostName = authHostName;
        return this;
    }
    
    public ConnectionDetailsBuilder withAuthPort(int authPort) {
        this.authPort = authPort;
        return this;
    }
    
    public ConnectionDetailsBuilder withAuthScheme(String authScheme) {
        this.authScheme = authScheme;
        return this;
    }
    
    public ConnectionDetailsBuilder withAuthUriRoot(String authUriRoot) {
        this.authUriRoot = authUriRoot;
        return this;
    }
    
    public ConnectionDetailsBuilder withOpsHostName(String opsHostName) {
        this.opsHostName = opsHostName;
        return this;
    }
    
    public ConnectionDetailsBuilder withOpsPort(int opsPort) {
        this.opsPort = opsPort;
        return this;
    }
    
    public ConnectionDetailsBuilder withOpsScheme(String opsScheme) {
        this.opsScheme = opsScheme;
        return this;
    }
    
    public ConnectionDetailsBuilder withOpsUriRoot(String opsUriRoot) {
        this.opsUriRoot = opsUriRoot;
        return this;
    }
    
    public ConnectionDetailsBuilder withConsumerCredential(String consumerCredential) {
        this.consumerCredential = consumerCredential;
        return this;
    }
    
    public ConnectionDetailsBuilder withThrottleSleepTime(long throttleSleepTime) {
        this.throttleSleepTime = throttleSleepTime;
        return this;
    }
    
    public ConnectionDetailsBuilder withMaxRetriesCount(int maxRetriesCount) {
        this.maxRetriesCount = maxRetriesCount;
        return this;
    }
    
    public ConnectionDetails build() {
        return new ConnectionDetails(connectionTimeout, readTimeout, 
                authHostName, authPort, authScheme, authUriRoot, 
                opsHostName, opsPort, opsScheme, opsUriRoot, 
                consumerCredential, throttleSleepTime, maxRetriesCount);
    }
}

