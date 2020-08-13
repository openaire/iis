package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;
import eu.dnetlib.iis.wf.referenceextraction.RetryLimitExceededException;

/**
 * HTTP based content retriever.
 * @author mhorst
 *
 */
public class HttpContentRetriever implements ContentRetriever {


    private static final long serialVersionUID = -6879262115292175343L;
    
    private static final Logger log = Logger.getLogger(HttpContentRetriever.class);
    
    private static final int retryDelayMillis = 10000;
    
    private static final int maxRetriesCount = 10;
    
    
    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url, int connectionTimeout, int readTimeout,
            int maxPageContentLength) {
        long startTime = System.currentTimeMillis();

        String currentUrl = url.toString();

        log.info("starting content retrieval for url: " + currentUrl);
        try {
            return retrieveUrlContent(currentUrl, connectionTimeout, readTimeout, maxPageContentLength, 0);
        } catch (Exception e) {
            log.error("content retrieval failed for url: " + currentUrl, e);
            return new ContentRetrieverResponse(e);
        } finally {
            log.info("finished content retrieval for url: " + currentUrl + " in "
                    + (System.currentTimeMillis() - startTime) + " ms");
        }
    }
    
    // ----------------------------------------- PRIVATE ------------------------------------------------
    
    /**
     * Builds HTTP client issuing requests to remote endpoints.
     */
    protected static HttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectionTimeout)
                .setConnectionRequestTimeout(connectionTimeout).setSocketTimeout(readTimeout).build());
        return httpClientBuilder.build();
    }
    
    private ContentRetrieverResponse retrieveUrlContent(String currentUrl, int connectionTimeout, int readTimeout,
            int maxPageContentLength, int retryCount) throws MalformedURLException, IOException, InterruptedException {
        
        if (retryCount > maxRetriesCount) {
            // FIXME indicate this kind of errors to prevent from storing in cache, handle it within CachedWebCrawler
            return new ContentRetrieverResponse(new RetryLimitExceededException(
                    String.format("number of maximum retries exceeded: '%d' for url: %s", maxRetriesCount, currentUrl)));
        }
        
        HttpURLConnection conn = getConnection(new URL(currentUrl), connectionTimeout, readTimeout);
        
        int statusCode = conn.getResponseCode();
        
        switch (statusCode) {
        case HttpURLConnection.HTTP_OK: {
            return readPageContent(conn, maxPageContentLength);
        }
        case HttpURLConnection.HTTP_NOT_FOUND: {
            return new ContentRetrieverResponse(new NoSuchElementException("unable to find page at: " + currentUrl));
        }
        case HttpURLConnection.HTTP_MOVED_TEMP:
        case HttpURLConnection.HTTP_MOVED_PERM:
        case HttpURLConnection.HTTP_SEE_OTHER: {
            String redirectedUrl = conn.getHeaderField("Location");
            if (StringUtils.isNotBlank(redirectedUrl)) {
                log.info("redirecting to: " + redirectedUrl);
                conn.disconnect();
                return retrieveUrlContent(redirectedUrl, connectionTimeout, readTimeout, maxPageContentLength,
                        ++retryCount);
            } else {
                return new ContentRetrieverResponse(
                        new RuntimeException("resource was moved, missing redirect header for the url: " + currentUrl));
            }
        }
        case 429: {
            // rete-limit hit
            log.warn(String.format("got %d response code, rate limit reached, delaying for %d ms", statusCode,
                    retryDelayMillis));
            Thread.sleep(retryDelayMillis);
            return retrieveUrlContent(currentUrl, connectionTimeout, readTimeout, maxPageContentLength, ++retryCount);
        }
        default: {
            return new ContentRetrieverResponse(new RuntimeException(String.format(
                    "got unsupported HTTP response code: %d when accessing page at url: %s", statusCode, currentUrl)));
        }

        }
    }
    
    private static HttpURLConnection getConnection(URL url, int connectionTimeout, int readTimeout) throws MalformedURLException, IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(readTimeout);
        conn.setConnectTimeout(connectionTimeout);
        return conn;
    }
    
    private static ContentRetrieverResponse readPageContent(HttpURLConnection conn, int maxPageContentLength) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder pageContent = new StringBuilder();
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                if (pageContent.length() < maxPageContentLength) {
                    if (pageContent.length() > 0) {
                        pageContent.append('\n');    
                    }
                    pageContent.append(inputLine);    
                } else {
                    log.warn("page content from URL: " + conn.getURL() + " exceeded maximum page length limit: " + maxPageContentLength + ", returning truncated page content");
                    return new ContentRetrieverResponse(pageContent.toString());
                }
            }
            return new ContentRetrieverResponse(pageContent.toString());
        }
    }

}
