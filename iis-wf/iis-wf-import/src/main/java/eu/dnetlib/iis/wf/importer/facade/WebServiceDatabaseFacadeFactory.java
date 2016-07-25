package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATABASE_SERVICE_LOCATION;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.*;

import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * WebService Database service facade factory.
 * 
 * @author mhorst
 *
 */
public class WebServiceDatabaseFacadeFactory implements ServiceFacadeFactory<DatabaseFacade> {

    
    private static final String IMPORT_DATABASE_CLIENT_CONNECTION_TIMEOUT = "import.database.client.connection.timeout";
    
    private static final String IMPORT_DATABASE_CLIENT_READ_TIMEOUT = "import.database.client.read.timeout";
    
    private static final String defaultDdatabaseConnectionTimeout = "60000";
    
    private static final String defaultDatabaseReadTimeout = "60000";
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public DatabaseFacade instantiate(Map<String, String> parameters) {
        Preconditions.checkArgument(parameters.containsKey(IMPORT_DATABASE_SERVICE_LOCATION), 
                "unknown database service location: no parameter provided:  '%s'", IMPORT_DATABASE_SERVICE_LOCATION);
        
        return new WebServiceDatabaseFacade(parameters.get(IMPORT_DATABASE_SERVICE_LOCATION), 
                ServiceFacadeUtils.getValue(parameters, IMPORT_DATABASE_CLIENT_CONNECTION_TIMEOUT, defaultDdatabaseConnectionTimeout),
                ServiceFacadeUtils.getValue(parameters, IMPORT_DATABASE_CLIENT_READ_TIMEOUT, defaultDatabaseReadTimeout),
                Long.parseLong(ServiceFacadeUtils.getValue(parameters, IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT, RESULTSET_READ_TIMEOUT_DEFAULT_VALUE)),
                Integer.parseInt(ServiceFacadeUtils.getValue(parameters, IMPORT_RESULT_SET_PAGESIZE, RESULTSET_PAGESIZE_DEFAULT_VALUE)));
    }

}
