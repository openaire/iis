package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_MAX_FILE_SIZE_MB;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECT_STORE_S3_ENDPOINT;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;

/**
 * Encapsulates all content retrieval parameters.
 * @author mhorst
 *
 */
public class ContentRetrievalContext {
    

    /**
     * Connection timeout.
     */
    private final int connectionTimeout;

    /**
     * Read timeout.
     */
    private final int readTimeout;
    
    /**
     * Maximum allowed file size expressed in KB.
     */
    private final long maxFileSizeKB;
    
    /**
     * Optional S3 client endpoint. 
     * 
     * To be set whenever environment is aware of S3 server existence.
     */
    private AmazonS3 s3Client;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public ContentRetrievalContext(Configuration cfg) {

        this(cfg.getInt(IMPORT_CONTENT_CONNECTION_TIMEOUT, 60000),
                cfg.getInt(IMPORT_CONTENT_READ_TIMEOUT, 60000),
                WorkflowRuntimeParameters.getIntegerParamValue(
                        IMPORT_CONTENT_MAX_FILE_SIZE_MB, cfg));

        // setting optional S3 context
        this.s3Client = initializeS3Client(cfg);
    }

    public ContentRetrievalContext(int connectionTimeout, int readTimeout, Integer maxFileSizeMB) {
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;

        // handling maximum content size
        if (maxFileSizeMB != null) {
            this.maxFileSizeKB = 1024l * maxFileSizeMB;
        } else {
            this.maxFileSizeKB = Long.MAX_VALUE;
        }
    }
    
    //------------------------ GETTERS --------------------------

    public int getConnectionTimeout() {
        return connectionTimeout;
    }


    public int getReadTimeout() {
        return readTimeout;
    }


    public long getMaxFileSizeKB() {
        return maxFileSizeKB;
    }


    public AmazonS3 getS3Client() {
        return s3Client;
    }
    
    public void setS3Client(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    //------------------------ PRIVATE --------------------------
    
    /**
     * Provides S3 client or null when context is unaware of S3 server.
     * 
     */
    private AmazonS3 initializeS3Client(Configuration cfg) {
        String endpointLocation = WorkflowRuntimeParameters.getParamValue(IMPORT_CONTENT_OBJECT_STORE_S3_ENDPOINT, cfg);
        if (endpointLocation != null) {
            return new AmazonS3Client(new HadoopBasedS3CredentialsProvider(cfg),
                    new ClientConfiguration().withConnectionTimeout(connectionTimeout).withSocketTimeout(readTimeout)
                            .withProtocol(Protocol.HTTPS)).withEndpoint(endpointLocation);
        } else {
            return null;
        }
    }
    
}
