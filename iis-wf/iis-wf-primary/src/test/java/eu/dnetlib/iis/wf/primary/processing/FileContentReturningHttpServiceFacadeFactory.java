package eu.dnetlib.iis.wf.primary.processing;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple factory producing {@link FacadeContentRetriever} for http content.
 */
public class FileContentReturningHttpServiceFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

    @Override
    public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
        Map<String, String> urlToClasspathMap = new HashMap<>();
        urlToClasspathMap.put("https://github.com/madgik/madis",
                "/eu/dnetlib/iis/wf/primary/processing/sampledataproducer/input/html/madis.html");

        return new FacadeContentRetriever<String, String>() {

            @Override
            protected String buildUrl(String objToBuildUrl) {
                return objToBuildUrl;
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                String classPathLocation = urlToClasspathMap.get(url);
                if (classPathLocation != null) {
                    return FacadeContentRetrieverResponse.success(ClassPathResourceProvider.getResourceContent(
                            classPathLocation));
                }
                return FacadeContentRetrieverResponse.success("");
            }
        };
    }
}
