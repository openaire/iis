package eu.dnetlib.iis.wf.importer.facade;

import javax.xml.ws.wsaddressing.W3CEndpointReference;

import eu.dnetlib.data.mdstore.MDStoreService;
import eu.dnetlib.data.mdstore.MDStoreServiceException;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

/**
 * WebService based MDStore facade.
 * 
 * @author mhorst
 *
 */
public class WebServiceMDStoreFacade extends AbstractResultSetAwareWebServiceFacade<MDStoreService> implements MDStoreFacade {

    
    //------------------------ CONSTRUCTORS -------------------
    
    /**
     * @param serviceLocation MDStore webservice location
     * @param serviceReadTimeout service read timeout
     * @param serviceConnectionTimeout service connection timeout
     * @param resultSetReadTimeout resultset read timeout
     * @param resultSetConnectionTimeout result set connection timeout
     * @param resultSetPageSize resultset page size
     */
    public WebServiceMDStoreFacade(String serviceLocation, 
            long serviceReadTimeout, long serviceConnectionTimeout,
            long resultSetReadTimeout, long resultSetConnectionTimeout, int resultSetPageSize) {
        super(MDStoreService.class, serviceLocation, 
                serviceReadTimeout, serviceConnectionTimeout, 
                resultSetReadTimeout, resultSetConnectionTimeout, resultSetPageSize);
    }
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public Iterable<String> deliverMDRecords(String mdStoreId) throws ServiceFacadeException {
        try {
            W3CEndpointReference eprResult = getService().deliverMDRecords(mdStoreId, null, null, null);
            ResultSetClientFactory rsFactory = new ResultSetClientFactory(
                    getResultSetPageSize(), getResultSetReadTimeout(), getResultSetConnectionTimeout());
            rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
            return rsFactory.getClient(eprResult);
        } catch (MDStoreServiceException e) {
            throw new ServiceFacadeException("delivering records for md store " + mdStoreId + " failed!", e);
        }
    }

}
