package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CLIENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CLIENT_READ_TIMEOUT;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CONNECTION_TIMEOUT_DEFAULT_VALUE;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_READ_TIMEOUT_DEFAULT_VALUE;

import java.util.Map;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;

/**
 * Url based streaming facade factory.
 * @author mhorst
 *
 */
public class ContextUrlStreamingFacadeFactory implements ServiceFacadeFactory<ContextStreamingFacade> {

    protected static final String IMPORT_CONTEXT_STREAM_ENDPOINT_URL = "import.context.stream.endpoint.url";
    

    //------------------------ LOGIC --------------------------
    
    @Override
    public ContextStreamingFacade instantiate(Map<String, String> parameters) {

        Preconditions.checkArgument(parameters.containsKey(IMPORT_CONTEXT_STREAM_ENDPOINT_URL),
                "unknown projects stream endpoint url, required parameter '%s' is missing!",
                IMPORT_CONTEXT_STREAM_ENDPOINT_URL);

        return new ContextUrlStreamingFacade(parameters.get(IMPORT_CONTEXT_STREAM_ENDPOINT_URL),
                Integer.parseInt(WorkflowRuntimeParameters.getParamValue(DNET_SERVICE_CLIENT_READ_TIMEOUT,
                        DNET_SERVICE_READ_TIMEOUT_DEFAULT_VALUE, parameters)),
                Integer.parseInt(WorkflowRuntimeParameters.getParamValue(DNET_SERVICE_CLIENT_CONNECTION_TIMEOUT,
                        DNET_SERVICE_CONNECTION_TIMEOUT_DEFAULT_VALUE, parameters)));

    }

    
}
