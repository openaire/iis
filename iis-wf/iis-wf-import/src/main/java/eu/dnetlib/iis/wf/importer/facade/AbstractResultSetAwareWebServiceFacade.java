package eu.dnetlib.iis.wf.importer.facade;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

/**
 * Abstract class utilized by all WebService facades.
 * @author mhorst
 * 
 */
public abstract class AbstractResultSetAwareWebServiceFacade<T> {

    /**
     * Web service.
     */
    protected final T service;
    
    /**
     * ResultSet read timeout.
     */
    protected final long resultSetReadTimeout;
    
    /**
     * ResultSet page size.
     */
    protected final int resultSetPageSize;
    
    //------------------------ CONSTRUCTORS -------------------
    
    /**
     * Instantiates underlying service.
     * @param clazz webservice class
     * @param serviceLocation webservice location
     * @param resultSetReadTimeout resultset read timeout
     * @param resultSetPageSize resultset page size
     */
    protected AbstractResultSetAwareWebServiceFacade(Class<T> clazz, String serviceLocation, 
            long resultSetReadTimeout, int resultSetPageSize) {
        W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
        eprBuilder.address(serviceLocation);
        eprBuilder.build();
        this.service = new JaxwsServiceResolverImpl().getService(clazz, eprBuilder.build());
        this.resultSetReadTimeout = resultSetReadTimeout;
        this.resultSetPageSize = resultSetPageSize;
    }
    
}
