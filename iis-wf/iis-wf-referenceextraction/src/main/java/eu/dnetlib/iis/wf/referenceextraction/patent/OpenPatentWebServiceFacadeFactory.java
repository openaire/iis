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

    public static final String PARAM_CONSUMER_KEY = "authnConsumerKey";
    public static final String PARAM_CONSUMER_SECRET = "authnConsumerSecret";

    public static final String PARAM_SERVICE_ENDPOINT_AUTH_HOST = "endpointAuthHost";
    public static final String PARAM_SERVICE_ENDPOINT_AUTH_PORT = "endpointAuthPort";
    public static final String PARAM_SERVICE_ENDPOINT_AUTH_SCHEME = "endpointAuthScheme";
    public static final String PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT = "endpointAuthUriRoot";

    public static final String PARAM_SERVICE_ENDPOINT_OPS_HOST = "endpointOpsHost";
    public static final String PARAM_SERVICE_ENDPOINT_OPS_PORT = "endpointOpsPort";
    public static final String PARAM_SERVICE_ENDPOINT_OPS_SCHEME = "endpointOpsScheme";
    public static final String PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT = "endpointOpsUriRoot";

    public static final String PARAM_SERVICE_ENDPOINT_READ_TIMEOUT = "endpointReadTimeout";
    public static final String PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT = "endpointConnectionTimeout";
    
    public static final String PARAM_SERVICE_ENDPOINT_THROTTLE_SLEEP_TIME = "endpointThrottleSleepTime";
    public static final String PARAM_SERVICE_ENDPOINT_RETRIES_COUNT = "endpointRetriesCount";
    
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

        String connectionTimeout = conf.getOrDefault(PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT, "60000");
        String readTimeout = conf.getOrDefault(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT, "60000");
        String throttleSleepTime = conf.getOrDefault(PARAM_SERVICE_ENDPOINT_THROTTLE_SLEEP_TIME, "10000");
        String retriesCount = conf.getOrDefault(PARAM_SERVICE_ENDPOINT_RETRIES_COUNT, "10");
                
        return new OpenPatentWebServiceFacade(ConnectionDetailsBuilder.newBuilder()
                .withConnectionTimeout(Integer.parseInt(connectionTimeout))
                .withReadTimeout(Integer.parseInt(readTimeout))
                .withAuthHostName(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_HOST))
                .withAuthPort(Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_PORT)))
                .withAuthScheme(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME))
                .withAuthUriRoot(conf.get(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT))
                .withOpsHostName(conf.get(PARAM_SERVICE_ENDPOINT_OPS_HOST))
                .withOpsPort(Integer.parseInt(conf.get(PARAM_SERVICE_ENDPOINT_OPS_PORT)))
                .withOpsScheme(conf.get(PARAM_SERVICE_ENDPOINT_OPS_SCHEME))
                .withOpsUriRoot(conf.get(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT))
                .withConsumerCredential(buildCredential(conf.get(PARAM_CONSUMER_KEY), conf.get(PARAM_CONSUMER_SECRET)))
                .withThrottleSleepTime(Long.parseLong(throttleSleepTime))
                .withMaxRetriesCount(Integer.parseInt(retriesCount)).build());
    }

    protected static String buildCredential(String key, String secret) {
        return Base64.getEncoder().encodeToString((key + ':' + secret).getBytes(DEFAULT_CHARSET));
    }

}
