package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;
import eu.dnetlib.iis.wf.referenceextraction.RetryLimitExceededException;

/**
 * HTTP based content retriever.
 * 
 * @author mhorst
 *
 */
public class HttpContentRetriever implements ContentRetriever {

    private static final long serialVersionUID = -6879262115292175343L;
    
    private static final Logger log = Logger.getLogger(HttpContentRetriever.class);
    
    private static final String HEADER_LOCATION = "Location";
    
    private int connectionTimeout;
    
    private int readTimeout;
    
    private int maxPageContentLength;
    
    private long throttleSleepTime;
    
    private int maxRetriesCount;
    
    // to be reinitialized after deserialization
    private transient CloseableHttpClient httpClient;

    
    // ----------------------------------------- CONSTRUCTORS ---------------------------------------
    
    
    public HttpContentRetriever(int connectionTimeout, int readTimeout, int maxPageContentLength,
            long throttleSleepTime, int maxRetriesCount) {
        initialize(connectionTimeout, readTimeout, maxPageContentLength, throttleSleepTime, maxRetriesCount);
    }

    
    // ----------------------------------------- LOGIC ----------------------------------------------
    
    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url) {

        long startTime = System.currentTimeMillis();

        log.info("starting content retrieval for url: " + url);
        try {
            return retrieveUrlContent(url.toString(), connectionTimeout, readTimeout, maxPageContentLength, 0);
        } catch (Exception e) {
            log.error("content retrieval failed for url: " + url, e);
            return new ContentRetrieverResponse(e);
        } finally {
            log.info("finished content retrieval for url: " + url + " in " + (System.currentTimeMillis() - startTime)
                    + " ms");
        }
    }
    
    // ----------------------------------------- PRIVATE ------------------------------------------------
    
    private ContentRetrieverResponse retrieveUrlContent(String currentUrl, int connectionTimeout, int readTimeout,
            int maxPageContentLength, int retryCount) throws MalformedURLException, IOException, InterruptedException {
        
        //FIXME method params to be dropped
        
        if (retryCount > maxRetriesCount) {
            // FIXME indicate this kind of errors to prevent from storing in cache, handle it within CachedWebCrawler
            String message = String.format("number of maximum retries exceeded: '%d' for url: %s", maxRetriesCount, currentUrl);
            log.error(message);
            return new ContentRetrieverResponse(new RetryLimitExceededException(message));
        }
        
        try (CloseableHttpResponse httpResponse = httpClient.execute(new HttpGet(currentUrl))) {
            
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            
            switch (statusCode) {
            case HttpURLConnection.HTTP_OK: {
                return readPageContent(httpResponse.getEntity(), maxPageContentLength, currentUrl);
            }
            case HttpURLConnection.HTTP_NOT_FOUND: {
                return new ContentRetrieverResponse(new NoSuchElementException("unable to find page at: " + currentUrl));
            }
            case HttpURLConnection.HTTP_MOVED_TEMP:
            case HttpURLConnection.HTTP_MOVED_PERM:
            case HttpURLConnection.HTTP_SEE_OTHER: {
                String redirectedUrl = getHeaderValue(httpResponse.getAllHeaders(), HEADER_LOCATION);
                if (StringUtils.isNotBlank(redirectedUrl)) {
                    log.info("redirecting to: " + redirectedUrl);
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
                        throttleSleepTime));
                Thread.sleep(throttleSleepTime);
                return retrieveUrlContent(currentUrl, connectionTimeout, readTimeout, maxPageContentLength, ++retryCount);
            }
            default: {
                return new ContentRetrieverResponse(new RuntimeException(String.format(
                        "got unsupported HTTP response code: %d when accessing page at url: %s", statusCode, currentUrl)));
            }
            }
        }
    }
    
    private static String getHeaderValue(Header[] headers, String headerName) {
        if (headers != null) {
            for (int i = 0; i < headers.length; i++) {
                if (headerName.equals(headers[i].getName())) {
                    return headers[i].getValue();
                }
            }    
        }
        return null;
    }
    
    private static ContentRetrieverResponse readPageContent(HttpEntity httpEntity, int maxPageContentLength, String url) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(httpEntity.getContent(), StandardCharsets.UTF_8))) {
            StringBuilder pageContent = new StringBuilder();
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                if (pageContent.length() < maxPageContentLength) {
                    if (pageContent.length() > 0) {
                        pageContent.append('\n');    
                    }
                    pageContent.append(inputLine);    
                } else {
                    log.warn(String.format(
                            "page content from URL: '%s' exceeded page length limit: %d, returning truncated page content",
                            url, maxPageContentLength));
                    return new ContentRetrieverResponse(pageContent.toString());
                }
            }
            return new ContentRetrieverResponse(pageContent.toString());
        }
    }
    
    /**
     * Builds HTTP client issuing requests to SH endpoint.
     */
    protected CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        // FIXME there are 3 similar methods in other classes, move it to a shared place
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectionTimeout)
                .setConnectionRequestTimeout(connectionTimeout).setSocketTimeout(readTimeout).build());
        return httpClientBuilder.build();
    }
    
    /**
     * Initializes the object either during construction or deserialization.
     */
    private void initialize(int connectionTimeout, int readTimeout, int maxPageContentLength, long throttleSleepTime, int maxRetriesCount) {
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
        this.httpClient = buildHttpClient(connectionTimeout, readTimeout);
        this.maxPageContentLength = maxPageContentLength;
        this.throttleSleepTime = throttleSleepTime;
        this.maxRetriesCount = maxRetriesCount;
    }
    

    // -------------------------- SerDe --------------------------------
    
    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(this.connectionTimeout);
        oos.writeObject(this.readTimeout);
        oos.writeObject(this.maxPageContentLength);
        oos.writeObject(this.throttleSleepTime);
        oos.writeObject(this.maxRetriesCount);
    }
    
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        initialize((Integer) ois.readObject(), (Integer) ois.readObject(), (Integer) ois.readObject(),
                (Long) ois.readObject(), (Integer) ois.readObject());
    }

}
