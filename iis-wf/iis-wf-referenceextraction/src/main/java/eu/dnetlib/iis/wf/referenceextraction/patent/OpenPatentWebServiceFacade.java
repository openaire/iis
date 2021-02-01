package eu.dnetlib.iis.wf.referenceextraction.patent;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.DEFAULT_CHARSET;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.Objects;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.wf.importer.HttpClientUtils;

/**
 * Remote EPO endpoint based patent service facade.
 * 
 * @author mhorst
 *
 */
public class OpenPatentWebServiceFacade extends FacadeContentRetriever<ImportedPatent, String> {
    
    private static final long serialVersionUID = -9154710658560662015L;

    private static final Logger log = LoggerFactory.getLogger(OpenPatentWebServiceFacade.class);
    
    private String authUriRoot;
    
    private String opsUriRoot;
    
    // security related 
    private String currentSecurityToken;

    private String consumerCredential;
    
    private long throttleSleepTime;
    
    private int maxRetriesCount;

    // fields to be reinitialized after deserialization
    private transient JsonParser jsonParser;

    private transient CloseableHttpClient httpClient;

    private transient HttpHost authHost;

    private transient HttpHost opsHost;
    
    /**
     * Serialization / deserialization details.
     */
    private SerDe serDe;

    
    // ------------------- CONSTRUCTORS ------------------------
    
    public OpenPatentWebServiceFacade(ConnectionDetails connDetails) {
        
        this(buildHttpClient(connDetails.getConnectionTimeout(), connDetails.getReadTimeout()),
                new HttpHost(connDetails.getAuthHostName(), connDetails.getAuthPort(), connDetails.getAuthScheme()),
                connDetails.getAuthUriRoot(),
                new HttpHost(connDetails.getOpsHostName(), connDetails.getOpsPort(), connDetails.getOpsScheme()),
                connDetails.getOpsUriRoot(), connDetails.getConsumerCredential(), connDetails.getThrottleSleepTime(),
                connDetails.getMaxRetriesCount(), new JsonParser());
        
        // persisting for further serialization and deserialization
        this.serDe = new SerDe(connDetails.getConnectionTimeout(), connDetails.getReadTimeout(),
                connDetails.getAuthHostName(), connDetails.getAuthPort(), connDetails.getAuthScheme(),
                connDetails.getOpsHostName(), connDetails.getOpsPort(), connDetails.getOpsScheme());
    }
    
    /**
     * Using this constructor simplifies object instantiation but also makes the whole object non-serializable.
     */
    protected OpenPatentWebServiceFacade(CloseableHttpClient httpClient, HttpHost authHost, String authUriRoot, HttpHost opsHost,
            String opsUriRoot, String consumerCredential, long throttleSleepTime, int maxRetriesCount,
            JsonParser jsonParser) {
        this.httpClient = httpClient;
        this.authHost = authHost;
        this.authUriRoot = authUriRoot;
        this.opsHost = opsHost;
        this.opsUriRoot = opsUriRoot;
        this.consumerCredential = consumerCredential;
        this.throttleSleepTime = throttleSleepTime;
        this.maxRetriesCount = maxRetriesCount;
        this.jsonParser = jsonParser;
    }

    /**
     * Builds url to query for a patent.
     *
     * @param objToBuildUrl Patent metadata for building query url.
     * @return Url to query for specific patent.
     */
    @Override
    protected String buildUrl(ImportedPatent objToBuildUrl) {
        StringBuilder strBuilder = new StringBuilder(opsUriRoot);
        if (!opsUriRoot.endsWith("/")) {
            strBuilder.append('/');
        }
        strBuilder.append(objToBuildUrl.getPublnAuth());
        strBuilder.append('.');
        strBuilder.append(objToBuildUrl.getPublnNr());
        strBuilder.append('.');
        strBuilder.append(objToBuildUrl.getPublnKind());
        strBuilder.append("/biblio");
        return strBuilder.toString();
    }

    /**
     * Retrieves patent metadata from EPO endpoint.
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

        HttpRequest httpRequest = buildPatentMetaRequest(url, getSecurityToken());
        try (CloseableHttpResponse httpResponse = httpClient.execute(opsHost, httpRequest)) {
            int statusCode = httpResponse.getStatusLine().getStatusCode();

            switch (statusCode) {
                case HttpURLConnection.HTTP_OK: {
                    HttpEntity entity = httpResponse.getEntity();
                    if (!isResponseEntityValid(entity)) {
                        return FacadeContentRetrieverResponse.persistentFailure(new PatentWebServiceFacadeException(
                                "got empty entity in response, server response: " + httpResponse));
                    }
                    return FacadeContentRetrieverResponse.success(EntityUtils.toString(entity));
                }
                case HttpURLConnection.HTTP_BAD_REQUEST: {
                    log.info("got 400 HTTP code in response, potential reason: access token invalid or expired, server response: {}",
                            httpResponse);
                    reauthenticate();
                    return retrieveContentOrThrow(url, ++retryCount);
                }
                case HttpURLConnection.HTTP_FORBIDDEN: {
                    log.warn("got 403 HTTP code in response, potential reason: endpoint rate limit reached. Delaying for {} ms, server response: {}",
                            throttleSleepTime, httpResponse);
                    Thread.sleep(throttleSleepTime);
                    return retrieveContentOrThrow(url, ++retryCount);
                }
                case HttpURLConnection.HTTP_NOT_FOUND: {
                    return FacadeContentRetrieverResponse.persistentFailure(new PatentWebServiceFacadeException(
                            "unable to find element at: " + httpRequest.getRequestLine()));
                }
                default: {
                    return FacadeContentRetrieverResponse.transientFailure(new PatentWebServiceFacadeException(String.format(
                            "got unhandled HTTP status code when accessing endpoint: %d, full status: %s, server response: %s",
                            statusCode, httpResponse.getStatusLine(), httpResponse)));
                }
            }
        }
    }

    // -------------------------- PRIVATE -------------------------

    private static Boolean isResponseEntityValid(HttpEntity entity){
        return Objects.nonNull(entity);
    }

    private void reinitialize(SerDe serDe, String authUriRoot, String opsUriRoot,
            String consumerCredential, long throttleSleepTime, int maxRetriesCount) {

        this.serDe = serDe;
        
        this.httpClient = buildHttpClient(serDe.connectionTimeout, serDe.readTimeout);
        this.authHost = new HttpHost(serDe.authHostName, serDe.authPort, serDe.authScheme);
        this.opsHost = new HttpHost(serDe.opsHostName, serDe.opsPort, serDe.opsScheme);
        
        this.authUriRoot = authUriRoot;
        this.opsUriRoot = opsUriRoot;
        
        this.consumerCredential = consumerCredential;
        this.throttleSleepTime = throttleSleepTime;
        this.maxRetriesCount = maxRetriesCount;
        
        this.jsonParser = new JsonParser();
    }
    
    /**
     * Builds HTTP client issuing requests to SH endpoint.
     */
    protected static CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        return HttpClientUtils.buildHttpClient(connectionTimeout, readTimeout);
    }
    
    protected String getSecurityToken() throws Exception {
        if (StringUtils.isBlank(this.currentSecurityToken)) {
            reauthenticate();
        }
        return currentSecurityToken;
    }

    protected void reauthenticate() throws Exception {
        currentSecurityToken = authenticate();
    }

    /**
     * Handles authentication operation resulting in a generation of security token.
     * Never returns null.
     * 
     * @throws IOException
     */
    private String authenticate() throws Exception {

        try (CloseableHttpResponse httpResponse = httpClient.execute(authHost, buildAuthRequest(consumerCredential, authUriRoot))) {
            int statusCode = httpResponse.getStatusLine().getStatusCode();

            if (statusCode == 200) {
                String jsonContent = IOUtils.toString(httpResponse.getEntity().getContent(), DEFAULT_CHARSET);

                JsonObject jsonObject = jsonParser.parse(jsonContent).getAsJsonObject();
                JsonElement accessToken = jsonObject.get("access_token");

                if (accessToken == null) {
                    throw new PatentWebServiceFacadeException("access token is missing: " + jsonContent);
                } else {
                    return accessToken.getAsString();
                }
            } else {
                throw new PatentWebServiceFacadeException(String.format(
                        "Authentication failed! HTTP status code when accessing endpoint: %d, full status: %s, server response: %s",
                        statusCode, httpResponse.getStatusLine(), EntityUtils.toString(httpResponse.getEntity())));
            } 
        }
    }

    protected static HttpRequest buildAuthRequest(String consumerCredential, String uriRoot) {
        HttpPost httpRequest = new HttpPost(uriRoot);
        httpRequest.addHeader("Authorization", "Basic " + consumerCredential);
        BasicNameValuePair grantType = new BasicNameValuePair("grant_type", "client_credentials");
        httpRequest.setEntity(new UrlEncodedFormEntity(Collections.singletonList(grantType), DEFAULT_CHARSET));
        return httpRequest;
    }

    protected static HttpRequest buildPatentMetaRequest(String url, String bearerToken) {
        HttpGet httpRequest = new HttpGet(url);
        httpRequest.addHeader("Authorization", "Bearer " + bearerToken);
        return httpRequest;
    }

    // -------------------------- SerDe --------------------------------
    
    private void writeObject(ObjectOutputStream oos) throws IOException {
        if (this.serDe == null) {
            throw new IOException("unable to serialize object: "
                    + "http connection related details are missing!");
        }
        oos.defaultWriteObject();
        oos.writeObject(this.serDe);
        oos.writeObject(this.authUriRoot);
        oos.writeObject(this.opsUriRoot);
        oos.writeObject(this.consumerCredential);
        oos.writeObject(this.throttleSleepTime);
        oos.writeObject(this.maxRetriesCount);
    }
    
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        reinitialize((SerDe) ois.readObject(), 
                (String) ois.readObject(), (String) ois.readObject(), 
                (String) ois.readObject(), (Long) ois.readObject(), (Integer) ois.readObject());
    }
    
    // -------------------------- INNER CLASS --------------------------
    
    static class SerDe implements Serializable {
        
        private static final long serialVersionUID = 1289144732356257009L;
        
        // http client related
        private int connectionTimeout;

        private int readTimeout;
        
        // EPO endpoints
        private String authHostName;
        
        private int authPort;

        private String authScheme;
        
        private String opsHostName;    
        
        private int opsPort;
        
        private String opsScheme;
        
        public SerDe(int connectionTimeout, int readTimeout, 
                String authHostName, int authPort, String authScheme,
                String opsHostName, int opsPort, String opsScheme) {
            this.connectionTimeout = connectionTimeout;
            this.readTimeout = readTimeout;
            this.authHostName = authHostName;
            this.authPort = authPort;
            this.authScheme = authScheme;
            this.opsHostName = opsHostName;
            this.opsPort = opsPort;
            this.opsScheme = opsScheme;
        }
    }

}
