package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_ISLOOKUP_SERVICE_LOCATION;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_PAGESIZE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.RESULTSET_PAGESIZE_DEFAULT_VALUE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.RESULTSET_READ_TIMEOUT_DEFAULT_VALUE;

import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * WebService Database service facade factory.
 * 
 * @author mhorst
 *
 */
public class WebServiceISLookupFacadeFactory implements ServiceFacadeFactory<ISLookupFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ISLookupFacade instantiate(Map<String, String> parameters) {
        Preconditions.checkArgument(parameters.containsKey(IMPORT_ISLOOKUP_SERVICE_LOCATION), 
                "unknown ISLookup service location: no parameter provided:  '%s'", IMPORT_ISLOOKUP_SERVICE_LOCATION);
        
        return new WebServiceISLookupFacade(parameters.get(IMPORT_ISLOOKUP_SERVICE_LOCATION), 
                Long.parseLong(ServiceFacadeUtils.getValue(parameters, IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT, RESULTSET_READ_TIMEOUT_DEFAULT_VALUE)),
                Integer.parseInt(ServiceFacadeUtils.getValue(parameters, IMPORT_RESULT_SET_PAGESIZE, RESULTSET_PAGESIZE_DEFAULT_VALUE)));
    }

}
