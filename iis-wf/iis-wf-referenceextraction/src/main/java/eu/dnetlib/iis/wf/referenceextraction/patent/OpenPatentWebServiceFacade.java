package eu.dnetlib.iis.wf.referenceextraction.patent;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.DEFAULT_CHARSET;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.NoSuchElementException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;

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
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
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
    
    private String authUriRoot;
    
    private String opsUriRoot;
    
    // security related 
    private String currentSecurityToken;

    private String consumerCredential;
    

    private long throttleSleepTime; 

    // fields to be reinitialized after deserialization
    private transient JsonParser jsonParser;

    private transient HttpClient httpClient;

    private transient HttpHost authHost;

    private transient HttpHost opsHost;
    
    /**
     * Serialization / deserialization details.
     */
    private SerDe serDe;
    
    
    // ------------------- CONSTRUCTORS ------------------------
    
    public OpenPatentWebServiceFacade(int connectionTimeout, int readTimeout, 
            String authHostName, int authPort, String authScheme, String authUriRoot,
            String opsHostName, int opsPort, String opsScheme, String opsUriRoot, 
            String consumerCredential, long throttleSleepTime) {
        
        this(buildHttpClient(connectionTimeout, readTimeout),
                new HttpHost(authHostName, authPort, authScheme), authUriRoot, 
                new HttpHost(opsHostName, opsPort, opsScheme), opsUriRoot,
                consumerCredential, throttleSleepTime, new JsonParser());
        
        // persisting for further serialization and deserialization
        this.serDe = new SerDe(connectionTimeout, readTimeout, 
                authHostName, authPort, authScheme, 
                opsHostName, opsPort, opsScheme);
    }
    
    /**
     * Using this constructor simplifies object instantiation but also makes the whole object non-serializable.
     */
    protected OpenPatentWebServiceFacade(HttpClient httpClient, HttpHost authHost, String authUriRoot,
            HttpHost opsHost, String opsUriRoot, String consumerCredential, long throttleSleepTime,
            JsonParser jsonParser) {
        this.httpClient = httpClient;
        this.authHost = authHost;
        this.authUriRoot = authUriRoot;
        this.opsHost = opsHost;
        this.opsUriRoot = opsUriRoot;
        this.consumerCredential = consumerCredential;
        this.throttleSleepTime = throttleSleepTime;
        this.jsonParser = jsonParser;
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
    
    // -------------------------- PRIVATE -------------------------

    private void reinitialize(SerDe serDe, String authUriRoot, String opsUriRoot,
            String consumerCredential, long throttleSleepTime) {

        this.serDe = serDe;
        
        this.httpClient = buildHttpClient(serDe.connectionTimeout, serDe.readTimeout);
        this.authHost = new HttpHost(serDe.authHostName, serDe.authPort, serDe.authScheme);
        this.opsHost = new HttpHost(serDe.opsHostName, serDe.opsPort, serDe.opsScheme);
        
        this.authUriRoot = authUriRoot;
        this.opsUriRoot = opsUriRoot;
        
        this.consumerCredential = consumerCredential;
        this.throttleSleepTime = throttleSleepTime;
        
        this.jsonParser = new JsonParser();
    }
    
    /**
     * Builds HTTP client issuing requests to SH endpoint.
     */
    protected static HttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParams, connectionTimeout);
        HttpConnectionParams.setSoTimeout(httpParams, readTimeout);
        return disableHostNameVerification(new DefaultHttpClient(httpParams));
    }
    
    private static HttpClient disableHostNameVerification(HttpClient httpClient) {
        HostnameVerifier hostnameVerifier = org.apache.http.conn.ssl.SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;

        SchemeRegistry registry = new SchemeRegistry();
        SSLSocketFactory socketFactory = SSLSocketFactory.getSocketFactory();
        socketFactory.setHostnameVerifier((X509HostnameVerifier) hostnameVerifier);
        registry.register(new Scheme("https", socketFactory, 443));
        SingleClientConnManager mgr = new SingleClientConnManager(httpClient.getParams(), registry);
        DefaultHttpClient httpClientDisabledHostNameVerification = new DefaultHttpClient(mgr, httpClient.getParams());

        // Set verifier     
        HttpsURLConnection.setDefaultHostnameVerifier(hostnameVerifier);
        return httpClientDisabledHostNameVerification;
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
    }
    
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        reinitialize((SerDe) ois.readObject(), 
                (String) ois.readObject(), (String) ois.readObject(), 
                (String) ois.readObject(), (Long) ois.readObject());
    }
    
    // -------------------------- INNER CLASS --------------------------
    
    class SerDe implements Serializable {
        
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
