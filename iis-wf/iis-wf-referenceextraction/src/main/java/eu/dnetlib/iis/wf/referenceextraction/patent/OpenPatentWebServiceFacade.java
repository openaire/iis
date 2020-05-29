package eu.dnetlib.iis.wf.referenceextraction.patent;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.DEFAULT_CHARSET;

import java.io.IOException;
import java.util.Collections;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;

/**
 * Remote EPO endpoint based patent service facade.
 * 
 * @author mhorst
 *
 */
public class OpenPatentWebServiceFacade implements PatentServiceFacade {
    
    private static final long serialVersionUID = -9154710658560662015L;

    private static final int DEFAULT_MAX_RETRIES_COUNT = 10;
    
    private static final Logger log = Logger.getLogger(OpenPatentWebServiceFacade.class);

    private String currentSecurityToken;

    private final String consumerCredential;
    
    private final HttpClient httpClient;

    private final HttpHost authHost;

    private final HttpHost opsHost;

    private final String authUriRoot;

    private final String opsUriRoot;
    
    private final long throttleSleepTime; 

    private final JsonParser jsonParser = new JsonParser();

    // ------------------- CONSTRUCTOR -------------------------
    
    public OpenPatentWebServiceFacade(HttpClient httpClient, HttpHost authHost, String authUriRoot,
            HttpHost opsHost, String opsUriRoot, String consumerCredential, long throttleSleepTime) {
        this.httpClient = httpClient;
        this.authHost = authHost;
        this.authUriRoot = authUriRoot;
        this.opsHost = opsHost;
        this.opsUriRoot = opsUriRoot;
        this.consumerCredential = consumerCredential;
        this.throttleSleepTime = throttleSleepTime;
    }

    // ------------------- LOGIC----------------------------
    
    @Override
    public String getPatentMetadata(ImportedPatent patent) throws Exception {
        return getPatentMetadata(patent, getSecurityToken(), 0);
    }

    // ------------------- PRIVATE -------------------------
    
    private String getPatentMetadata(ImportedPatent patent, String securityToken, int retryCount) throws Exception {
        
        if (retryCount > DEFAULT_MAX_RETRIES_COUNT) {
            throw new PatentServiceException("number of maximum retries exceeded: " + DEFAULT_MAX_RETRIES_COUNT);
        }

        HttpRequest httpRequest = buildPatentMetaRequest(patent, securityToken, opsUriRoot);
        HttpResponse httpResponse = httpClient.execute(opsHost, httpRequest);

        int statusCode = httpResponse.getStatusLine().getStatusCode();

        switch (statusCode) {
        case 200: {
            HttpEntity entity = httpResponse.getEntity();
            if (entity != null) {
                return EntityUtils.toString(entity);
            } else {
                throw new PatentServiceException("got empty response, full status: " + httpResponse.getStatusLine()
                        + ", server response: " + EntityUtils.toString(httpResponse.getEntity()));
            }
        }
        case 400: {
            // access token invalid or expired, reauthenticating, incrementing retryCount to prevent from authn loop
            return getPatentMetadata(patent, reauthenticate(), ++retryCount);
        }
        case 403: {
            // quota exceeded or resource blacklisted or account blacklisted
            log.warn("got 403 HTTP code in response, potential reason: endpoint rate limit reached. Delaying for "
                    + throttleSleepTime + " ms, server response: " + EntityUtils.toString(httpResponse.getEntity()));
            Thread.sleep(throttleSleepTime);
            return getPatentMetadata(patent, securityToken, ++retryCount);
        }
        case 404: {
            throw new NoSuchElementException("unable to find element at: " + httpRequest.getRequestLine());
        }
        default: {
            String errMessage = "got unhandled HTTP status code when accessing endpoint: " + statusCode
                    + ", full status: " + httpResponse.getStatusLine() + ", server response: "
                    + EntityUtils.toString(httpResponse.getEntity());
            throw new PatentServiceException(errMessage);
        }
        }
    }

    protected String getSecurityToken() throws Exception {
        if (StringUtils.isNotBlank(this.currentSecurityToken)) {
            return currentSecurityToken;
        } else {
            return reauthenticate();
        }
    }

    protected String reauthenticate() throws Exception {
        currentSecurityToken = authenticate();
        return currentSecurityToken;
    }

    /**
     * Handles authentication operation resulting in a generation of security token.
     * Never returns null.
     * 
     * @throws IOException
     */
    private String authenticate() throws Exception {

        HttpResponse httpResponse = httpClient.execute(authHost, buildAuthRequest(consumerCredential, authUriRoot));

        int statusCode = httpResponse.getStatusLine().getStatusCode();

        if (statusCode == 200) {
            String jsonContent = IOUtils.toString(httpResponse.getEntity().getContent(), DEFAULT_CHARSET);

            JsonObject jsonObject = jsonParser.parse(jsonContent).getAsJsonObject();
            JsonElement accessToken = jsonObject.get("access_token");

            if (accessToken == null) {
                throw new PatentServiceException("access token is missing: " + jsonContent);
            } else {
                return accessToken.getAsString();
            }
        } else {
            String errMessage = "Authentication failed! HTTP status code when accessing endpoint: " + statusCode
                    + ", full status: " + httpResponse.getStatusLine() + ", server response: "
                    + EntityUtils.toString(httpResponse.getEntity());
            throw new PatentServiceException(errMessage);
        }
        
    }

    protected static HttpRequest buildAuthRequest(String consumerCredential, String uriRoot) {
        HttpPost httpRequest = new HttpPost(uriRoot);
        httpRequest.addHeader("Authorization", "Basic " + consumerCredential);
        BasicNameValuePair grantType = new BasicNameValuePair("grant_type", "client_credentials");
        httpRequest.setEntity(new UrlEncodedFormEntity(Collections.singletonList(grantType), DEFAULT_CHARSET));
        return httpRequest;
    }

    protected static HttpRequest buildPatentMetaRequest(ImportedPatent patent, String bearerToken, String urlRoot) {
        HttpGet httpRequest = new HttpGet(getPatentMetaUrl(patent, urlRoot));
        httpRequest.addHeader("Authorization", "Bearer " + bearerToken);
        return httpRequest;
    }

    protected static String getPatentMetaUrl(ImportedPatent patent, String urlRoot) {
        StringBuilder strBuilder = new StringBuilder(urlRoot);
        if (!urlRoot.endsWith("/")) {
            strBuilder.append('/');
        }
        strBuilder.append(patent.getPublnAuth());
        strBuilder.append('.');
        strBuilder.append(patent.getPublnNr());
        strBuilder.append('.');
        strBuilder.append(patent.getPublnKind());
        strBuilder.append("/biblio");
        return strBuilder.toString();
    }

}
