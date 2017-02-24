package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CLIENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CLIENT_READ_TIMEOUT;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_CONNECTION_TIMEOUT_DEFAULT_VALUE;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DNET_SERVICE_READ_TIMEOUT_DEFAULT_VALUE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_PAGESIZE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.RESULTSET_CONNECTION_TIMEOUT_DEFAULT_VALUE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.RESULTSET_PAGESIZE_DEFAULT_VALUE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.RESULTSET_READ_TIMEOUT_DEFAULT_VALUE;

import java.util.Map;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;

/**
 * WebService MDStore service facade factory.
 * 
 * @author mhorst
 *
 */
public class WebServiceMDStoreFacadeFactory implements ServiceFacadeFactory<MDStoreFacade> {


    //------------------------ LOGIC --------------------------
    
    @Override
    public WebServiceMDStoreFacade instantiate(Map<String, String> parameters) {
        Preconditions.checkArgument(parameters.containsKey(IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION), 
                "unknown MDStore service location: no parameter provided:  '%s'", IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION);

        return new WebServiceMDStoreFacade(parameters.get(IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION),
                Long.parseLong(WorkflowRuntimeParameters.getParamValue(DNET_SERVICE_CLIENT_READ_TIMEOUT, DNET_SERVICE_READ_TIMEOUT_DEFAULT_VALUE, parameters)),
                Long.parseLong(WorkflowRuntimeParameters.getParamValue(DNET_SERVICE_CLIENT_CONNECTION_TIMEOUT, DNET_SERVICE_CONNECTION_TIMEOUT_DEFAULT_VALUE, parameters)),
                Long.parseLong(WorkflowRuntimeParameters.getParamValue(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT, RESULTSET_READ_TIMEOUT_DEFAULT_VALUE, parameters)),
                Long.parseLong(WorkflowRuntimeParameters.getParamValue(IMPORT_RESULT_SET_CLIENT_CONNECTION_TIMEOUT, RESULTSET_CONNECTION_TIMEOUT_DEFAULT_VALUE, parameters)),
                Integer.parseInt(WorkflowRuntimeParameters.getParamValue(IMPORT_RESULT_SET_PAGESIZE, RESULTSET_PAGESIZE_DEFAULT_VALUE, parameters)));
    }

}
