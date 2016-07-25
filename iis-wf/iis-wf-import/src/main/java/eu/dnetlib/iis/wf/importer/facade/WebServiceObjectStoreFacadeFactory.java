package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECSTORE_PAGESIZE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECT_STORE_LOC;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;

import java.util.Map;

import com.google.common.base.Preconditions;

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
                Long.parseLong(parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)),
                Integer.parseInt(parameters.get(IMPORT_CONTENT_OBJECSTORE_PAGESIZE)));
    }

}
