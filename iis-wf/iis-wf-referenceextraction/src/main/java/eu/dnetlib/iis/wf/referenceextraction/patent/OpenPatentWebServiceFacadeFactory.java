package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * RESTful Open Patent WebService based facade factory.
 * 
 * @author mhorst
 *
 */
public class OpenPatentWebServiceFacadeFactory implements ServiceFacadeFactory<PatentServiceFacade> {

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
    public PatentServiceFacade instantiate(Map<String, String> conf) {

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

        String connectionTimeout = conf.containsKey(PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT)
                ? conf.get(PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT)
                : "60000";
        String readTimeout = conf.containsKey(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT)
                ? conf.get(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT)
                : "60000";
        String throttleSleepTime = conf.containsKey(PARAM_SERVICE_ENDPOINT_THROTTLE_SLEEP_TIME)
                ? conf.get(PARAM_SERVICE_ENDPOINT_THROTTLE_SLEEP_TIME)
                : "10000";
                
        
        return new OpenPatentWebServiceFacade(Integer.parseInt(connectionTimeout), Integer.parseInt(readTimeout),
                conf.get(PARAM_SERVICE_ENDPOINT_AUTH_HOST),Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_PORT)),
                conf.get(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME), conf.get(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT),
                conf.get(PARAM_SERVICE_ENDPOINT_OPS_HOST), Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_OPS_PORT)),
                conf.get(PARAM_SERVICE_ENDPOINT_OPS_SCHEME), conf.get(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT),
                buildCredential(conf.get(PARAM_CONSUMER_KEY), conf.get(PARAM_CONSUMER_SECRET)),
                Long.parseLong(throttleSleepTime));
    }

    protected static String buildCredential(String key, String secret) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(key);
        strBuilder.append(':');
        strBuilder.append(secret);
        return Base64.getEncoder().encodeToString(strBuilder.toString().getBytes(DEFAULT_CHARSET));
    }

}
