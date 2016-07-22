package eu.dnetlib.iis.wf.importer.facade;

import java.util.Map;

import javax.xml.ws.BindingProvider;
import javax.xml.ws.wsaddressing.W3CEndpointReference;

import eu.dnetlib.enabling.database.rmi.DatabaseService;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

/**
 * WebService based database facade.
 * 
 * @author mhorst
 *
 */
public class WebServiceDatabaseFacade extends AbstractWebServiceFacade<DatabaseService> implements DatabaseFacade {

    private final long resultSetReadTimeout;
    
    private final int defaultPagesize = 100;
    
    //------------------------ CONSTRUCTORS -------------------
    
    /**
     * @param serviceLocation database service location
     * @param databaseConnectionTimeout database connection timeout
     * @param databaseConnectionReadTimeout database read timeout
     * @param resultSetReadTimeout result set providing database results read timeout
     */
    public WebServiceDatabaseFacade(String serviceLocation, 
            String databaseConnectionTimeout, String databaseConnectionReadTimeout, long resultSetReadTimeout) {
        super(DatabaseService.class, serviceLocation);
        this.resultSetReadTimeout = resultSetReadTimeout;
        Map<String, Object> requestContext = ((BindingProvider) service).getRequestContext();
        requestContext.put("javax.xml.ws.client.connectionTimeout", databaseConnectionTimeout);
        requestContext.put("javax.xml.ws.client.receiveTimeout", databaseConnectionReadTimeout);
    }

    //------------------------ LOGIC --------------------------
    
    @Override
    public Iterable<String> searchSQL(String databaseName, String query) throws ServiceFacadeException {
        W3CEndpointReference eprResult = service.searchSQL(databaseName, query);
        // obtaining resultSet
        ResultSetClientFactory rsFactory = new ResultSetClientFactory();
        rsFactory.setTimeout(resultSetReadTimeout);  
        rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
        rsFactory.setPageSize(defaultPagesize);
        return rsFactory.getClient(eprResult);
    }

}
