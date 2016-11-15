package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECT_STORE_LOC;
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
 * WebService ObjectStore facade factory.
 * 
 * @author mhorst
 *
 */
public class WebServiceObjectStoreFacadeFactory implements ServiceFacadeFactory<ObjectStoreFacade> {

    //------------------------ LOGIC --------------------------
    
    @Override
    public WebServiceObjectStoreFacade instantiate(Map<String, String> parameters) {
        Preconditions.checkArgument(parameters.containsKey(IMPORT_CONTENT_OBJECT_STORE_LOC), 
                "unknown object store service location: no parameter provided:  '%s'", IMPORT_CONTENT_OBJECT_STORE_LOC);

        return new WebServiceObjectStoreFacade(parameters.get(IMPORT_CONTENT_OBJECT_STORE_LOC),
                Long.parseLong(WorkflowRuntimeParameters.getParamValue(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT, RESULTSET_READ_TIMEOUT_DEFAULT_VALUE, parameters)),
                Long.parseLong(WorkflowRuntimeParameters.getParamValue(IMPORT_RESULT_SET_CLIENT_CONNECTION_TIMEOUT, RESULTSET_CONNECTION_TIMEOUT_DEFAULT_VALUE, parameters)),
                Integer.parseInt(WorkflowRuntimeParameters.getParamValue(IMPORT_RESULT_SET_PAGESIZE, RESULTSET_PAGESIZE_DEFAULT_VALUE, parameters)));
    }

}
