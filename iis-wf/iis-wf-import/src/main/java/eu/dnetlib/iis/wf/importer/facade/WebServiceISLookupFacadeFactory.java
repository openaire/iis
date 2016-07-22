package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.*;

import java.security.InvalidParameterException;
import java.util.Map;

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
        if (!parameters.containsKey(IMPORT_ISLOOKUP_SERVICE_LOCATION)) {
            throw new InvalidParameterException("unknown ISLookup service location, "
                    + "required parameter '" + IMPORT_ISLOOKUP_SERVICE_LOCATION + "' is missing!");
        }
        return new WebServiceISLookupFacade(parameters.get(IMPORT_ISLOOKUP_SERVICE_LOCATION), 
                Long.parseLong(parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)));
    }

}
