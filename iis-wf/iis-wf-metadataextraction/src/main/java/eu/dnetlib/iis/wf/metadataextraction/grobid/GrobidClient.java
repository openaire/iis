package eu.dnetlib.iis.wf.metadataextraction.grobid;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
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
     * Base sleep time for the first retry (exponential backoff).
     */
    private long throttleSleepTime;

    /**
     * Upper bound for a single retry sleep (ms).
     */
    private static final long MAX_RETRY_SLEEP_MILLIS = 30000L;

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

    /**
     * Parses a raw bibliographic citation string by relying on an external Grobid service.
     * @param citation raw citation string
     * @return The TEI XML result (containing a single {@code biblStruct}) as a string
     * @throws IOException If an error occurs during processing
     * @throws TransientException if temporary error occurred
     * @throws InterruptedException when interrupted while waiting during retry
     */
    public String processCitation(String citation) throws IOException, TransientException, InterruptedException {
        return processCitation(citation, 0);
    }

    /**
     * Parses a batch of raw bibliographic citation strings in a single request,
     * relying on the Grobid {@code /api/processCitationList} endpoint.
     *
     * @param citations raw citation strings (non-blank)
     * @return the TEI XML response containing a {@code <listBibl>} with one
     *         {@code <biblStruct>} per citation, in the same order; an empty
     *         string when no biblStruct could be produced (HTTP 204)
     * @throws IOException If an error occurs during processing
     * @throws TransientException if temporary error occurred
     * @throws InterruptedException when interrupted while waiting during retry
     */
    public String processCitationList(List<String> citations) throws IOException, TransientException, InterruptedException {
        return processCitationList(citations, 0);
    }

    private String processCitationList(List<String> citations, int retryCount) throws IOException, TransientException, InterruptedException {
        HttpPost httpPost = new HttpPost(grobidUrl + "/api/processCitationList");

        List<NameValuePair> params = new ArrayList<>(citations.size());
        for (String citation : citations) {
            params.add(new BasicNameValuePair("citations", citation));
        }
        httpPost.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {

            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode == HttpURLConnection.HTTP_OK) {
                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                    return EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
                } else {
                    throw new IOException("No response entity received from Grobid");
                }
            } else if (statusCode == HttpURLConnection.HTTP_NO_CONTENT) {
                // process completed, but no biblStruct could be produced
                return "";
            } else if (isPermanentFailure(statusCode)) {
                // throwing IOException to indicate permanent issue (client errors and 500)
                String error = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                throw new IOException("Grobid request failed with status code " + statusCode + ": " + error);
            } else {
                // throwing TransientException to indicate transient nature of the failure
                // those are usually 502, 503 and 504 HTTP error codes but it is possible other kind of failures may occur
                String error = response.getEntity() != null ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : null;
                String message = "Grobid request failed with status code " + statusCode + ": " + error;
                if (retryCount >= maxRetriesCount) {
                    throw new TransientException(message);
                } else {
                    retryCount++;
                    long sleepMillis = retrySleepMillis(response, retryCount);
                    logger.warn("{}. Retrying ({}/{} after waiting {} ms...)", message, retryCount, maxRetriesCount, sleepMillis);
                    Thread.sleep(sleepMillis);
                    return processCitationList(citations, retryCount);
                }
            }
        } catch (SocketTimeoutException e) {
            throw new TransientException("Socket timeout exceeded when communicating with the grobid server!", e);
        }
    }

    /**
     * Determines whether the given HTTP status code represents a permanent
     * failure that should not be retried: 500 (internal service error) and
     * 4xx client errors, except 408 (request timeout) and 429 (too many
     * requests) which are transient.
     */
    private static boolean isPermanentFailure(int statusCode) {
        return statusCode == HttpURLConnection.HTTP_INTERNAL_ERROR
                || (statusCode >= 400 && statusCode < 500 && statusCode != 408 && statusCode != 429);
    }

    /**
     * Computes the sleep time before a retry: honors a {@code Retry-After}
     * response header when present, otherwise uses exponential backoff with
     * jitter, starting from the configured throttle sleep time and capped.
     */
    private long retrySleepMillis(CloseableHttpResponse response, int retryCount) {
        Header retryAfter = response.getFirstHeader("Retry-After");
        if (retryAfter != null) {
            try {
                long seconds = Long.parseLong(retryAfter.getValue().trim());
                return Math.min(seconds * 1000L, MAX_RETRY_SLEEP_MILLIS);
            } catch (NumberFormatException e) {
                // malformed header - fall back to exponential backoff
            }
        }
        long base = Math.min(throttleSleepTime << (retryCount - 1), MAX_RETRY_SLEEP_MILLIS);
        long jitter = ThreadLocalRandom.current().nextLong(base / 2 + 1);
        return Math.max(1L, base - jitter);
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }    

    // -------------------------- PRIVATE --------------------------------------

    private String processPdfByteBuffer(ByteBuffer pdfByteBuffer, int retryCount) throws IOException, TransientException, InterruptedException {
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
                    String error = response.getEntity() != null ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : null;
                    String message = "Grobid request failed with status code " + statusCode + ": " + error;
                    if (retryCount >= maxRetriesCount) {
                        throw new TransientException(message);
                    } else {
                        retryCount++;
                        long sleepMillis = retrySleepMillis(response, retryCount);
                        logger.warn("{}. Retrying ({}/{} after waiting {} ms...)", message, retryCount, maxRetriesCount, sleepMillis);
                        Thread.sleep(sleepMillis);
                        pdfByteBuffer.rewind();
                        return processPdfByteBuffer(pdfByteBuffer, retryCount);
                    }
                }
            } catch (SocketTimeoutException e) {
                throw new TransientException("Socket timeout exceeded when communicating with the grobid server!", e);
            }
        }
    }

    private String processCitation(String citation, int retryCount) throws IOException, TransientException, InterruptedException {
        HttpPost httpPost = new HttpPost(grobidUrl + "/api/processCitation");

        // NOTE: this Grobid deployment (0.8.2) serves /api/processCitation as
        // application/x-www-form-urlencoded; multipart/form-data is rejected with 415.
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("citations", citation));
        httpPost.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {

            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode == HttpURLConnection.HTTP_OK) {
                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                    return EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
                } else {
                    throw new IOException("No response entity received from Grobid");
                }
            } else if (isPermanentFailure(statusCode)) {
                // throwing IOException to indicate permanent issue (client errors and 500)
                String error = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                throw new IOException("Grobid request failed with status code " + statusCode + ": " + error);
            } else {
                // throwing TransientException to indicate transient nature of the failure
                // those are usually 502, 503 and 504 HTTP error codes but it is possible other kind of failures may occur
                String error = response.getEntity() != null ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : null;
                String message = "Grobid request failed with status code " + statusCode + ": " + error;
                if (retryCount >= maxRetriesCount) {
                    throw new TransientException(message);
                } else {
                    retryCount++;
                    long sleepMillis = retrySleepMillis(response, retryCount);
                    logger.warn("{}. Retrying ({}/{} after waiting {} ms...)", message, retryCount, maxRetriesCount, sleepMillis);
                    Thread.sleep(sleepMillis);
                    return processCitation(citation, retryCount);
                }
            }
        } catch (SocketTimeoutException e) {
            throw new TransientException("Socket timeout exceeded when communicating with the grobid server!", e);
        }
    }
}
