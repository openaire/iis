package eu.dnetlib.iis.wf.importer.facade;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

/**
 * Abstract class utilized by all WebService facades.
 * @author mhorst
 * 
 */
public abstract class AbstractWebServiceFacade<T> {

    /**
     * Web service.
     */
    protected T service;
    
    //------------------------ CONSTRUCTORS -------------------
    
    /**
     * Instantiates underlying service.
     * @param clazz webservice class
     * @param serviceLocation webservice location
     */
    protected AbstractWebServiceFacade(Class<T> clazz, String serviceLocation) {
        W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
        eprBuilder.address(serviceLocation);
        eprBuilder.build();
        this.service = new JaxwsServiceResolverImpl().getService(clazz, eprBuilder.build());
    }
    
}
