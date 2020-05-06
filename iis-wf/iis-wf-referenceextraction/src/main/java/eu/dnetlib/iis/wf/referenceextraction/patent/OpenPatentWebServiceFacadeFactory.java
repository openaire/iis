package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

import com.google.common.base.Preconditions;

/**
 * RESTful Open Patent WebService based facade factory.
 * 
 * @author mhorst
 *
 */
public class OpenPatentWebServiceFacadeFactory implements PatentServiceFacadeFactory {

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

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
    
    public static final String PARAM_SERVICE_ENDPOINT_THROTTLE_SLEEP_TIME = "patent.service.endpoint.throttle.sleep.time";

    @Override
    public PatentServiceFacade create(Configuration conf) {

        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_CONSUMER_KEY)));
        Preconditions.checkArgument(StringUtils.isNotBlank(conf.get(PARAM_CONSUMER_SECRET)));

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
                conf.get(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT),
                buildCredential(conf.get(PARAM_CONSUMER_KEY), conf.get(PARAM_CONSUMER_SECRET)),
                Long.parseLong(conf.get(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT, "10000")));
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
