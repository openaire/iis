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
public class WebServiceDatabaseFacadeFactory implements ServiceFacadeFactory<DatabaseFacade> {

    
    private static final String IMPORT_DATABASE_CLIENT_CONNECTION_TIMEOUT = "import.database.client.connection.timeout";
    
    private static final String IMPORT_DATABASE_CLIENT_READ_TIMEOUT = "import.database.client.read.timeout";
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public DatabaseFacade instantiate(Map<String, String> parameters) {
        if (!parameters.containsKey(IMPORT_DATABASE_SERVICE_LOCATION)) {
            throw new InvalidParameterException("unknown database service location, "
                    + "required parameter '" + IMPORT_DATABASE_SERVICE_LOCATION + "' is missing!");
        }
        return new WebServiceDatabaseFacade(parameters.get(IMPORT_DATABASE_SERVICE_LOCATION), 
                parameters.get(IMPORT_DATABASE_CLIENT_CONNECTION_TIMEOUT), 
                parameters.get(IMPORT_DATABASE_CLIENT_READ_TIMEOUT), 
                Long.parseLong(parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)));
    }

}
