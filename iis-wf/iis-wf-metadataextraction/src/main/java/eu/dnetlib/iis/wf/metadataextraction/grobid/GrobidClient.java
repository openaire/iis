package eu.dnetlib.iis.wf.metadataextraction.grobid;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.iis.wf.importer.HttpClientUtils;
import eu.dnetlib.iis.wf.metadataextraction.TransientException;

/**
 * HTTP client communicating with Grobid server.
 * 
 * @author mhorst
 */
public class GrobidClient implements Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(GrobidClient.class);

    /**
     * Grobid server URL.
     */
    private final String grobidUrl;
    
    /**
     * HTTP client to be closed once the GrobidClient is closed. 
     */
    private final CloseableHttpClient httpClient;
    
    /**
     * Throttle sleep time for a single retry.
     */
    private long throttleSleepTime;

    /**
     * Maximum number of allowed retries before throwing exception.
     */
    private int maxRetriesCount;

    /**
     * Default constructor accepting Grobid server location as parameter.
     * @param grobidUrl grobid server location
     * @param connectionTimeout
     * @param readTimeout
     * @param throttleSleepTime
     * @param maxRetriesCount
     */
    public GrobidClient(String grobidUrl, int connectionTimeout, int readTimeout,
            long throttleSleepTime, int maxRetriesCount) {
        this.grobidUrl = grobidUrl;
        this.httpClient = HttpClientUtils.buildHttpClient(connectionTimeout, readTimeout);
        this.throttleSleepTime = throttleSleepTime;
        this.maxRetriesCount = maxRetriesCount;
    }
    
    // ------------------------------------- LOGIC ----------------------------------------------
        
    /**
     * Parses a PDF input stream by relying on an external Grobid service.
     * @param pdfByteBuffer PDF byte buffer
     * @return The TEI XML result as a string
     * @throws IOException If an error occurs during processing
     * @throws TransientException if temporary error occurred
     * @throws InterruptedException when interrupted while waiting during retry
     */
    public String processPdfByteBuffer(ByteBuffer pdfByteBuffer) throws IOException, TransientException, InterruptedException {
        return processPdfByteBuffer(pdfByteBuffer, 0);
    }
    
    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }    

    // -------------------------- PRIVATE --------------------------------------

    private String processPdfByteBuffer(ByteBuffer pdfByteBuffer, int retryCount) throws IOException, TransientException, InterruptedException {
        // need to rewind whenever retrying
        if (retryCount > 0) {
            pdfByteBuffer.rewind();
        }

        try (InputStream pdfInputStream = new ByteBufferInputStream(pdfByteBuffer)) {

            HttpPost httpPost = new HttpPost(grobidUrl + "/api/processFulltextDocument");

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();

            builder.addBinaryBody("input", pdfInputStream, ContentType.APPLICATION_OCTET_STREAM, null);

            // Add form parameters for raw citations and affiliations
            builder.addTextBody("includeRawCitations", "1", ContentType.TEXT_PLAIN);
            builder.addTextBody("includeRawAffiliations", "1", ContentType.TEXT_PLAIN);

            HttpEntity multipart = builder.build();
            httpPost.setEntity(multipart);

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {

                int statusCode = response.getStatusLine().getStatusCode();

                if (statusCode == HttpURLConnection.HTTP_OK) {
                    HttpEntity responseEntity = response.getEntity();
                    if (responseEntity != null) {
                        return EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
                    } else {
                        throw new IOException("No response entity received from Grobid");
                    }
                } else if (statusCode == HttpURLConnection.HTTP_INTERNAL_ERROR) {
                    // throwing IOException to indicate permanent issue
                    String error = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    throw new IOException("Grobid request failed with status code " + statusCode + ": " + error);
                } else {
                    // throwing TransientException to indicate transient nature of the failure
                    // those are usually 502, 503 and 504 HTTP error codes but it is possible other kind of failures may occur
                    String error = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    String message = "Grobid request failed with status code " + statusCode + ": " + error;
                    if (retryCount >= maxRetriesCount) {
                        throw new TransientException(message);
                    } else {
                        retryCount++;
                        logger.warn(message);
                        logger.warn("retrying for the " + retryCount + " time after waiting " + throttleSleepTime + " ms...");
                        Thread.sleep(throttleSleepTime);
                        return processPdfByteBuffer(pdfByteBuffer, retryCount);
                    }
                }
            } catch (SocketTimeoutException e) {
                throw new TransientException("Socket timeout exceeded when communicating with the grobid server!", e);
            }
        }
    }
}
