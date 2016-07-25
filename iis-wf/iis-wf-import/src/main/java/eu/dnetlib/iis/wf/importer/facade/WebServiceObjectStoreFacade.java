package eu.dnetlib.iis.wf.importer.facade;

import javax.xml.ws.wsaddressing.W3CEndpointReference;

import org.apache.log4j.Logger;

import eu.dnetlib.data.objectstore.rmi.ObjectStoreService;
import eu.dnetlib.data.objectstore.rmi.ObjectStoreServiceException;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

/**
 * WebService based MDStore facade.
 * 
 * @author mhorst
 *
 */
public class WebServiceObjectStoreFacade extends AbstractWebServiceFacade<ObjectStoreService> implements ObjectStoreFacade {

    private final Logger log = Logger.getLogger(this.getClass());
    
    private final long resultSetReadTimeout;
    
    private final int resultSetPageSize;
    
    //------------------------ CONSTRUCTORS -------------------
    
    /**
     * @param serviceLocation ObjectStore webservice location
     * @param resultSetReadTimeout resultset read timeout
     * @param resultSetPageSize resultset page size
     */
    public WebServiceObjectStoreFacade(String serviceLocation, long resultSetReadTimeout, int resultSetPageSize) {
        super(ObjectStoreService.class, serviceLocation);
        this.resultSetReadTimeout = resultSetReadTimeout;
        this.resultSetPageSize = resultSetPageSize;
    }
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public Iterable<String> deliverObjects(String objectStoreId, long from, long until) throws ServiceFacadeException {
        try {
            W3CEndpointReference eprResult = service.deliverObjects(objectStoreId, from, until);
            log.debug("processing object store: " + objectStoreId + " and obtained ResultSet EPR: " + eprResult.toString());
            // obtaining resultSet
            ResultSetClientFactory rsFactory = new ResultSetClientFactory();
            rsFactory.setTimeout(resultSetReadTimeout);  
            rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
            rsFactory.setPageSize(resultSetPageSize);
            return rsFactory.getClient(eprResult);
        } catch (ObjectStoreServiceException e) {
            throw new ServiceFacadeException("delivering records for object store " + objectStoreId + " failed!", e);
        }
    }

}
