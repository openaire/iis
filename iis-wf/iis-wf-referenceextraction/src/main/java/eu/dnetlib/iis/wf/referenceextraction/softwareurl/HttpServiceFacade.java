package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.iis.wf.importer.HttpClientUtils;

/**
 * HTTP based content retriever.
 * 
 * @author mhorst
 *
 */
public class HttpServiceFacade extends FacadeContentRetriever<String, String> {

    private static final long serialVersionUID = -6879262115292175343L;

    private static final Logger log = LoggerFactory.getLogger(HttpServiceFacade.class);

    private static final String HEADER_LOCATION = "Location";
    
    /**
     * HTTP Status-Code 429: Too many requests.
     */
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    
    private int connectionTimeout;
    
    private int readTimeout;
    
    private int maxPageContentLength;
    
    private long throttleSleepTime;
    
    private int maxRetriesCount;
    
    // to be reinitialized after deserialization
    private transient CloseableHttpClient httpClient;

    // ----------------------------------------- CONSTRUCTORS ---------------------------------------

    public HttpServiceFacade(int connectionTimeout, int readTimeout, int maxPageContentLength,
                             long throttleSleepTime, int maxRetriesCount) {
        initialize(connectionTimeout, readTimeout, maxPageContentLength, throttleSleepTime, maxRetriesCount);
    }

    // ----------------------------------------- PRIVATE ------------------------------------------------

    /**
     * Builds url to query for a http service.
     *
     * @param objToBuildUrl Url for querying.
     * @return Url to query for http service.
     */
    @Override
    protected String buildUrl(String objToBuildUrl) {
        return objToBuildUrl;
    }

    /**
     * Retrieves web page content from given url.
     * 
     * This method is recursive and requires response entity to be consumed in order
     * not to hit the ConnectionPoolTimeoutException when connecting the same host
     * more than 2 times within recursion (e.g. when reattepmting).
     */
    @Override
    protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
        if (retryCount > maxRetriesCount) {
            return failureWhenOverMaxRetries(url, maxRetriesCount);
        }

        try (CloseableHttpResponse httpResponse = httpClient.execute(new HttpGet(url))) {
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            
            switch (statusCode) {
                case HttpURLConnection.HTTP_OK: {
                    HttpEntity entity = httpResponse.getEntity();
                    if (!isResponseEntityValid(entity)) {
                        return FacadeContentRetrieverResponse.persistentFailure(new HttpServiceFacadeException(
                                "got empty entity in response, server response: " + httpResponse));
                    }
                    return readPageContent(entity, maxPageContentLength, url);
                }
                case HttpURLConnection.HTTP_MOVED_PERM:
                case HttpURLConnection.HTTP_MOVED_TEMP:
                case HttpURLConnection.HTTP_SEE_OTHER: {
                    String redirectedUrl = getHeaderValue(httpResponse.getAllHeaders(), HEADER_LOCATION);
                    if (StringUtils.isNotBlank(redirectedUrl)) {
                        log.info("got {} response code, redirecting to {}, server response: {}", statusCode,
                                redirectedUrl, httpResponse);
                        return retrieveContentOrThrow(redirectedUrl, ++retryCount);
                    }
                    return FacadeContentRetrieverResponse.persistentFailure(new HttpServiceFacadeException(
                            "resource was moved, missing redirect header for the url: " + url));
                }
                case HttpURLConnection.HTTP_NOT_FOUND: {
                    return FacadeContentRetrieverResponse.persistentFailure(new HttpServiceFacadeException(
                            "unable to find page at: " + url));
                }
                case HTTP_TOO_MANY_REQUESTS: {
                    log.warn("got {} response code, potential reason: rate limit reached. Delaying for {} ms, server response: {}",
                            statusCode, throttleSleepTime, httpResponse);
                    Thread.sleep(throttleSleepTime);
                    return retrieveContentOrThrow(url, ++retryCount);
                }
                default: {
                    return FacadeContentRetrieverResponse.persistentFailure(new HttpServiceFacadeException(String.format(
                            "got unsupported HTTP response code: %d when accessing page at url: %s", statusCode, url)));
                }
            }
        }
    }

    private static Boolean isResponseEntityValid(HttpEntity entity) {
        return Objects.nonNull(entity);
    }

    private static String getHeaderValue(Header[] headers, String headerName) {
        if (headers != null) {
            for (Header header : headers) {
                if (headerName.equals(header.getName())) {
                    return header.getValue();
                }
            }    
        }
        return null;
    }

    private static FacadeContentRetrieverResponse<String> readPageContent(HttpEntity httpEntity, int maxPageContentLength, String url) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(httpEntity.getContent(), StandardCharsets.UTF_8))) {
            StringBuilder pageContent = new StringBuilder();
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                if (inputLine.length() < maxPageContentLength && pageContent.length() < maxPageContentLength) {
                    if (pageContent.length() > 0) {
                        pageContent.append('\n');    
                    }
                    pageContent.append(inputLine);    
                } else {
                    log.warn("page content from URL: '{}' exceeded page length limit: {}, returning truncated page content",
                            url, maxPageContentLength);
                    return FacadeContentRetrieverResponse.success(pageContent.toString());
                }
            }
            return FacadeContentRetrieverResponse.success(pageContent.toString());
        }
    }
    
    /**
     * Builds HTTP client issuing requests to a remote endpoint.
     */
    protected CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        return HttpClientUtils.buildHttpClient(connectionTimeout, readTimeout);
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
