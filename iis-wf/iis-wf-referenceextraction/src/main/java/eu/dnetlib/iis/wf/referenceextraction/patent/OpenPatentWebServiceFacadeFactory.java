package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;

/**
 * RESTful Open Patent WebService based facade factory.
 * 
 * @author mhorst
 *
 */
public class OpenPatentWebServiceFacadeFactory implements PatentServiceFacadeFactory {

    private static final Logger log = Logger.getLogger(OpenPatentWebServiceFacadeFactory.class);

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private static final long DEFAULT_DELAY_MS = 1000;

    private static final int MAX_RETRIES_COUNT = 10;

    public static final String PARAM_CONSUMER_KEY = "patent.service.authn.consumer.key";
    public static final String PARAM_CONSUMER_SECRET = "patent.service.authn.consumer.secret";

    public static final String PARAM_SERVICE_ENDPOINT_AUTH_HOST = "patent.service.endpoint.auth.host";
    public static final String PARAM_SERVICE_ENDPOINT_AUTH_PORT = "patent.service.endpoint.auth.port";
    public static final String PARAM_SERVICE_ENDPOINT_AUTH_SCHEME = "patent.service.endpoint.auth.scheme";
    public static final String PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT = "patent.service.endpoint.auth.uri.root";

    public static final String PARAM_SERVICE_ENDPOINT_OPS_HOST = "patent.service.endpoint.ops.host";
    public static final String PARAM_SERVICE_ENDPOINT_OPS_PORT = "patent.service.endpoint.ops.port";
    public static final String PARAM_SERVICE_ENDPOINT_OPS_SCHEME = "patent.service.endpoint.ops.scheme";
    public static final String PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT = "patent.service.endpoint.ops.uri.root";

    public static final String PARAM_SERVICE_ENDPOINT_READ_TIMEOUT = "patent.service.endpoint.read.timeout";
    public static final String PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT = "patent.service.endpoint.connection.timeout";

    private String consumerCredential;

    @Override
    public PatentServiceFacade create(Configuration conf) {

        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_CONSUMER_KEY)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_CONSUMER_SECRET)));
        this.consumerCredential = buildCredential(conf.get(PARAM_CONSUMER_KEY), conf.get(PARAM_CONSUMER_SECRET));

        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_HOST)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_PORT)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT)));

        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_OPS_HOST)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_OPS_PORT)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_OPS_SCHEME)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT)));

        return new OpenPatentWebServiceFacade(
                buildHttpClient(Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT, "60000")),
                        Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT, "60000"))),
                new HttpHost(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_HOST),
                        Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_PORT)),
                        conf.get(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME)),
                conf.get(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT),
                new HttpHost(conf.get(PARAM_SERVICE_ENDPOINT_OPS_HOST),
                        Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_OPS_PORT)),
                        conf.get(PARAM_SERVICE_ENDPOINT_OPS_SCHEME)),
                conf.get(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT));
    }

    private class OpenPatentWebServiceFacade implements PatentServiceFacade {

        private String currentSecurityToken;

        private final HttpClient httpClient;

        private final HttpHost authHost;

        private final HttpHost opsHost;

        private final String authUriRoot;

        private final String opsUriRoot;

        private final JsonParser jsonParser = new JsonParser();

        public OpenPatentWebServiceFacade(HttpClient httpClient, HttpHost authHost, String authUriRoot,
                HttpHost opsHost, String opsUriRoot) {
            this.httpClient = httpClient;
            this.authHost = authHost;
            this.authUriRoot = authUriRoot;
            this.opsHost = opsHost;
            this.opsUriRoot = opsUriRoot;
        }

        @Override
        public String getPatentMetadata(ImportedPatent patent) throws Exception {
            return getPatentMetadata(patent, getSecurityToken(), 0);
        }

        public String getPatentMetadata(ImportedPatent patent, String securityToken, int retryCount) throws Exception {

            HttpRequest httpRequest = buildPatentMetaRequest(patent, securityToken);
            HttpResponse httpResponse = httpClient.execute(opsHost, httpRequest);

            int statusCode = httpResponse.getStatusLine().getStatusCode();

            if (statusCode == 200) {

                HttpEntity entity = httpResponse.getEntity();
                if (entity != null) {
                    return EntityUtils.toString(entity);
                } else {
                    throw new RuntimeException("got empty response, full status: " + httpResponse.getStatusLine()
                            + ", server response: " + EntityUtils.toString(httpResponse.getEntity()));
                }

            } else if (statusCode == 404) {
                
                throw new NoSuchElementException("unable to find element at: " + httpRequest.getRequestLine());
                
            } else if (statusCode == 400) {
                    // access token invalid or expired, reauthenticating, incrementing retryCount to prevent from authn loop
                    return getPatentMetadata(patent, reauthenticate(), retryCount++);
            } else if (statusCode == 429) {
                // got throttled, delaying...
                // TODO make sure it is 429, retrieve the delay ms from the response header or calculate it on whatever is present there
                log.warn("endpoint rate limit reached, delaying for " + DEFAULT_DELAY_MS + " ms, server response: "
                        + EntityUtils.toString(httpResponse.getEntity()));
                Thread.sleep(DEFAULT_DELAY_MS);
                return getPatentMetadata(patent, securityToken, retryCount);

            } else {
                String errMessage = "got unhandled HTTP status code when accessing endpoint: " + statusCode
                        + ", full status: " + httpResponse.getStatusLine() + ", server response: "
                        + EntityUtils.toString(httpResponse.getEntity());
                if (retryCount < MAX_RETRIES_COUNT) {
                    log.error(errMessage + ", number of retries left: " + (MAX_RETRIES_COUNT - retryCount));
                    Thread.sleep(DEFAULT_DELAY_MS);
                    return getPatentMetadata(patent, securityToken, retryCount++);
                } else {
                    throw new RuntimeException(errMessage);
                }
            }
        }

        private String getSecurityToken() throws IOException {
            if (StringUtils.isNotBlank(this.currentSecurityToken)) {
                return currentSecurityToken;
            } else {
                return reauthenticate();
            }
        }

        private String reauthenticate() throws IOException {
            currentSecurityToken = authenticate();
            return currentSecurityToken;
        }

        /**
         * Handles authentication operation resulting in a generation of security token.
         * 
         * @throws IOException
         */
        private String authenticate() throws IOException {

            HttpResponse httpResponse = httpClient.execute(authHost, buildAuthRequest());

            int statusCode = httpResponse.getStatusLine().getStatusCode();

            if (statusCode == 200) {
                String jsonContent = IOUtils.toString(httpResponse.getEntity().getContent(), DEFAULT_CHARSET);

                JsonObject jsonObject = jsonParser.parse(jsonContent).getAsJsonObject();
                JsonElement accessToken = jsonObject.get("access_token");

                if (accessToken == null) {
                    throw new RuntimeException("access token is missing: " + jsonContent);
                } else {
                    return accessToken.getAsString();
                }
            } else {
                String errMessage = "Authentication failed! HTTP status code when accessing endpoint: " + statusCode
                        + ", full status: " + httpResponse.getStatusLine() + ", server response: "
                        + EntityUtils.toString(httpResponse.getEntity());
                throw new RuntimeException(errMessage);
            }
            
        }

        private HttpRequest buildAuthRequest() {
            HttpPost httpRequest = new HttpPost(authUriRoot);
            httpRequest.addHeader("Authorization", "Basic " + consumerCredential);
            BasicNameValuePair grantType = new BasicNameValuePair("grant_type", "client_credentials");
            httpRequest.setEntity(new UrlEncodedFormEntity(Collections.singletonList(grantType), DEFAULT_CHARSET));
            return httpRequest;
        }

        private HttpRequest buildPatentMetaRequest(ImportedPatent patent, String bearerToken) {
            HttpGet httpRequest = new HttpGet(getPatentMetaUrl(patent));
            httpRequest.addHeader("Authorization", "Bearer " + bearerToken);
            return httpRequest;
        }

        private String getPatentMetaUrl(ImportedPatent patent) {
            StringBuilder strBuilder = new StringBuilder(opsUriRoot);
            if (!opsUriRoot.endsWith("/")) {
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

    protected static String buildCredential(String key, String secret) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(key);
        strBuilder.append(':');
        strBuilder.append(secret);
        return Base64.getEncoder().encodeToString(strBuilder.toString().getBytes(DEFAULT_CHARSET));
    }

    /**
     * Builds HTTP client issuing requests to SH endpoint.
     */
    protected static HttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParams, connectionTimeout);
        HttpConnectionParams.setSoTimeout(httpParams, readTimeout);
        return new DefaultHttpClient(httpParams);
    }

}
