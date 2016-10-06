package eu.dnetlib.iis.wf.importer.stream.project;

import java.net.MalformedURLException;
import java.util.Map;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Url based streaming facade factory.
 * @author mhorst
 *
 */
public class UrlStreamingFacadeFactory implements ServiceFacadeFactory<StreamingFacade> {

    private static final String IMPORT_PROJECT_STREAM_ENDPOINT_URL = "import.project.stream.endpoint.url";
    
    private static final String IMPORT_PROJECT_STREAM_COMPRESS = "import.project.stream.compress";
    

    //------------------------ LOGIC --------------------------
    
    @Override
    public StreamingFacade instantiate(Map<String, String> parameters) {
        try {
            Preconditions.checkArgument(parameters.containsKey(IMPORT_PROJECT_STREAM_ENDPOINT_URL), 
                    "unknown projects stream endpoint url, required parameter '%s' is missing!", IMPORT_PROJECT_STREAM_ENDPOINT_URL);
            
            boolean compress = false;
            if (parameters.containsKey(IMPORT_PROJECT_STREAM_COMPRESS)) {
                compress = Boolean.parseBoolean(parameters.get(IMPORT_PROJECT_STREAM_COMPRESS));
            }
            
            return new UrlStreamingFacade(parameters.get(IMPORT_PROJECT_STREAM_ENDPOINT_URL), compress);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    
}
