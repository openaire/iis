package eu.dnetlib.iis.wf.importer.facade;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Utility class facilitating {@link ContextUrlStreamingFacade}.
 * @author mhorst
 */
public class ContextUrlStreamingFacadeUtils {

    private static final String URL_SLASH = "/";
    

    /**
     * Builds an url for a given endpoint location and context identifier.
     * @param endpointLocation root url
     * @param contextId context identifier to be attached
     */
    public static String buildUrl(String endpointLocation, String contextId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(endpointLocation), "endpointLocation has not been set");
        Preconditions.checkArgument(StringUtils.isNotBlank(contextId), "contextId has not been set");
        
        StringBuilder urlBuilder = new StringBuilder(endpointLocation);
        if (!endpointLocation.endsWith(URL_SLASH)) {
            urlBuilder.append(URL_SLASH);
        }
        urlBuilder.append(contextId);
        return urlBuilder.toString();
    }
}
