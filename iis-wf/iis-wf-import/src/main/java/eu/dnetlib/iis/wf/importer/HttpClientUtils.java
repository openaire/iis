package eu.dnetlib.iis.wf.importer;

import java.io.Closeable;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * HTTP client utility class.
 * 
 * @author mhorst
 *
 */
public class HttpClientUtils {

    /**
     * Builds {@link Closeable} HTTP client issuing requests to remote endpoint.
     */
    public static CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectionTimeout)
                .setConnectionRequestTimeout(connectionTimeout).setSocketTimeout(readTimeout).build());
        return httpClientBuilder.build();
    }
    
}
