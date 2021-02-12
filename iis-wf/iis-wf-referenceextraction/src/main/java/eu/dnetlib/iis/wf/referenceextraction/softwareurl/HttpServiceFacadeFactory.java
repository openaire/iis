package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;

/**
 * Factory class instantiating {@link HttpServiceFacade}.
 * 
 * @author mhorst
 *
 */
public class HttpServiceFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>> {

    public static final String PARAM_READ_TIMEOUT = "readTimeout";
    public static final String PARAM_CONNECTION_TIMEOUT = "connectionTimeout";

    public static final String PARAM_MAX_PAGE_CONTENT_LENGTH = "maxPageContentLength";
    
    public static final String PARAM_THROTTLE_SLEEP_TIME = "throttleSleepTime";
    public static final String PARAM_RETRIES_COUNT = "retriesCount";
    

    @Override
    public FacadeContentRetriever<String, String> instantiate(Map<String, String> conf) {
        
        String connectionTimeout = conf.getOrDefault(PARAM_CONNECTION_TIMEOUT, "60000");
        String readTimeout = conf.getOrDefault(PARAM_READ_TIMEOUT, "60000");
        
        String maxPageContentLength = conf.getOrDefault(PARAM_MAX_PAGE_CONTENT_LENGTH, "500000");
        
        String throttleSleepTime = conf.getOrDefault(PARAM_THROTTLE_SLEEP_TIME, "10000");
        String retriesCount = conf.getOrDefault(PARAM_RETRIES_COUNT, "10");
        
        return new HttpServiceFacade(Integer.parseInt(connectionTimeout), Integer.parseInt(readTimeout),
                Integer.parseInt(maxPageContentLength), Long.parseLong(throttleSleepTime), Integer.parseInt(retriesCount));

    }

}
