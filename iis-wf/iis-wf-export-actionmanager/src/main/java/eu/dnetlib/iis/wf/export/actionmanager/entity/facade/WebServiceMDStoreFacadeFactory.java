package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ENTITY_MDSTORE_SERVICE_LOCATION;

import java.security.InvalidParameterException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.ProcessUtils;

/**
 * Web service based {@link MDStoreFacadeFactory} implementation.
 * @author mhorst
 *
 */
public class WebServiceMDStoreFacadeFactory implements MDStoreFacadeFactory {

    // -------------------- LOGIC -------------------------
    
    @Override
    public MDStoreFacade instantiate(Map<String, String> parameters) {
        String mdStoreLocation = ProcessUtils.getParameterValue(EXPORT_ENTITY_MDSTORE_SERVICE_LOCATION, null, parameters);
        if (StringUtils.isBlank(mdStoreLocation) || WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoreLocation)) {
            throw new InvalidParameterException("unable to export document entities to action manager, " + 
                    "unknown MDStore service location. "
                    + "Required parameter '" + EXPORT_ENTITY_MDSTORE_SERVICE_LOCATION + "' is missing!");
        } else {
            return new WebServiceMDStoreFacade(mdStoreLocation);
            
        }
    }

}
