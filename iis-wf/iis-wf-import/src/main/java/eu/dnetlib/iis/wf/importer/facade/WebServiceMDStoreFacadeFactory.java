package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATACITE_MDSTORE_PAGESIZE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;

import java.security.InvalidParameterException;
import java.util.Map;

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
        if (!parameters.containsKey(IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION)) {
            throw new InvalidParameterException("unknown MDStore service location, "
                    + "required parameter '" + IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION + "' is missing!");
        }
        return new WebServiceMDStoreFacade(parameters.get(IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION),
                Long.parseLong(parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)),
                Integer.parseInt(parameters.get(IMPORT_DATACITE_MDSTORE_PAGESIZE)));
    }

}
