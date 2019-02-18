package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry;

import com.cloudera.com.amazonaws.auth.AWSCredentials;
import com.cloudera.com.amazonaws.auth.AWSCredentialsProvider;
import com.cloudera.com.amazonaws.auth.AnonymousAWSCredentials;
import com.cloudera.com.amazonaws.auth.BasicAWSCredentials;

/**
 * S3 credentials provider relying on Hadoop credentials.
 * 
 * Capable of making use of hadoop.security.credential.provider.path property pointing to keystore with required credentials.
 * 
 * When either Hadoop credential provider or credential entries were not found then anonymous credentials are provided. 
 * 
 * @author mhorst
 *
 */
public class HadoopBasedS3CredentialsProvider implements AWSCredentialsProvider {

    private static String accessKey = "fs.s3a.access.key";
    
    private static String secretKey = "fs.s3a.secret.key";
    
    /**
     * List of provides to be queried for credentials.
     */
    private final List<CredentialProvider> providers;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor accepting hadoop configuration.
     */
    public HadoopBasedS3CredentialsProvider(Configuration cfg) {
        try {
            this.providers = CredentialProviderFactory.getProviders(cfg);
        } catch (IOException e) {
            throw new RuntimeException("unable to obtain credential providers", e);
        }
    }
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public AWSCredentials getCredentials() {
        if (!providers.isEmpty()) {
            for (CredentialProvider provider : providers) {
                try {
                    CredentialEntry accessEntry = provider.getCredentialEntry(accessKey);
                    CredentialEntry secretEntry = provider.getCredentialEntry(secretKey);
                    if (accessEntry != null && secretEntry != null) {
                        return new BasicAWSCredentials(
                                String.valueOf(accessEntry.getCredential()),
                                String.valueOf(secretEntry.getCredential()));
                    }
                } catch (IOException ioe) {
                    throw new RuntimeException("Can't get key entry from key provider of type: "
                            + provider.getClass().getName(), ioe);
                }
            }
        }
        // returning anonymous credentials when no hadoop credentials found
        return new AnonymousAWSCredentials();
    }

    @Override
    public void refresh() {
        throw new RuntimeException("unsupported operation");
    }

}
