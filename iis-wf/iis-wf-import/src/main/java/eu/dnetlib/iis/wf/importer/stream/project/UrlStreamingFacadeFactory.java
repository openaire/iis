package eu.dnetlib.iis.wf.importer.stream.project;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CLIENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CLIENT_READ_TIMEOUT;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CONNECTION_TIMEOUT_DEFAULT_VALUE;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_READ_TIMEOUT_DEFAULT_VALUE;

import java.net.MalformedURLException;
import java.util.Map;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
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
            
            return new UrlStreamingFacade(parameters.get(IMPORT_PROJECT_STREAM_ENDPOINT_URL), compress,
                    Integer.parseInt(WorkflowRuntimeParameters.getParamValue(DNET_SERVICE_CLIENT_READ_TIMEOUT, DNET_SERVICE_READ_TIMEOUT_DEFAULT_VALUE, parameters)),
                    Integer.parseInt(WorkflowRuntimeParameters.getParamValue(DNET_SERVICE_CLIENT_CONNECTION_TIMEOUT, DNET_SERVICE_CONNECTION_TIMEOUT_DEFAULT_VALUE, parameters)));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    
}
