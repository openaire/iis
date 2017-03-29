package eu.dnetlib.iis.wf.importer.facade;

import java.util.Map;

import javax.xml.ws.BindingProvider;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.log4j.Logger;

import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

/**
 * Abstract class utilized by all WebService facades.
 * @author mhorst
 * 
 */
public abstract class AbstractResultSetAwareWebServiceFacade<T> {

    private final Logger log = Logger.getLogger(this.getClass());
    
    /**
     * Web service.
     */
    private final T service;
    
    /**
     * ResultSet read timeout.
     */
    private final long resultSetReadTimeout;
    
    /**
     * ResultSet connection timeout.
     */
    private final long resultSetConnectionTimeout;
    
    /**
     * ResultSet page size.
     */
    private final int resultSetPageSize;
    
    
    //------------------------ CONSTRUCTORS -------------------
    
    /**
     * Instantiates underlying service.
     * @param clazz webservice class
     * @param serviceLocation webservice location
     * @param serviceReadTimeout service read timeout
     * @param serviceConnectionTimeout service connection timeout
     * @param resultSetReadTimeout resultset read timeout
     * @param resultSetConnectionTimeout resultset connection timeout
     * @param resultSetPageSize resultset page size
     */
    protected AbstractResultSetAwareWebServiceFacade(Class<T> clazz, String serviceLocation,
            long serviceReadTimeout, long serviceConnectionTimeout, 
            long resultSetReadTimeout, long resultSetConnectionTimeout, int resultSetPageSize) {
        W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
        eprBuilder.address(serviceLocation);
        eprBuilder.build();
        this.service = new JaxwsServiceResolverImpl().getService(clazz, eprBuilder.build());
        if (this.service instanceof BindingProvider) {
            log.info(String.format("setting timeouts for %s: read timeout (%s) and connect timeout (%s)", 
                    BindingProvider.class, serviceReadTimeout, serviceConnectionTimeout));
            final Map<String, Object> requestContext = ((BindingProvider) service).getRequestContext();

            // can't be sure about which will be used. Set them all.
            requestContext.put("com.sun.xml.internal.ws.request.timeout", serviceReadTimeout);
            requestContext.put("com.sun.xml.internal.ws.connect.timeout", serviceConnectionTimeout);

            requestContext.put("com.sun.xml.ws.request.timeout", serviceReadTimeout);
            requestContext.put("com.sun.xml.ws.connect.timeout", serviceConnectionTimeout);

            requestContext.put("javax.xml.ws.client.receiveTimeout", serviceReadTimeout);
            requestContext.put("javax.xml.ws.client.connectionTimeout", serviceConnectionTimeout);
        }
        
        this.resultSetReadTimeout = resultSetReadTimeout;
        this.resultSetConnectionTimeout = resultSetConnectionTimeout;
        this.resultSetPageSize = resultSetPageSize;
    }

    
    //------------------------ GETTERS -------------------------

    public T getService() {
        return service;
    }


    public long getResultSetReadTimeout() {
        return resultSetReadTimeout;
    }


    public long getResultSetConnectionTimeout() {
        return resultSetConnectionTimeout;
    }


    public int getResultSetPageSize() {
        return resultSetPageSize;
    }
    
}
