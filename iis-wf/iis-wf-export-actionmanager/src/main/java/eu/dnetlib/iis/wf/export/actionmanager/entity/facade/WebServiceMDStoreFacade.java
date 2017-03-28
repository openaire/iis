package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import java.util.Map;

import javax.xml.ws.BindingProvider;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.log4j.Logger;

import eu.dnetlib.data.mdstore.MDStoreService;
import eu.dnetlib.data.mdstore.MDStoreServiceException;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

/**
 * Web service based {@link MDStoreFacade}.
 * 
 * @author mhorst
 *
 */
public class WebServiceMDStoreFacade implements MDStoreFacade {

    private static final Logger log = Logger.getLogger(WebServiceMDStoreFacade.class);
    
    /**
     * Web service.
     */
    protected final MDStoreService service;
    
    // -------------------- CONSTRUCTORS -------------------------
    
    public WebServiceMDStoreFacade(String serviceLocation,
            long serviceReadTimeout, long serviceConnectionTimeout) {
        W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
        eprBuilder.address(serviceLocation);
        eprBuilder.build();
        this.service = new JaxwsServiceResolverImpl().getService(MDStoreService.class, eprBuilder.build());
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
    }
    
    // -------------------- PUBLIC -------------------------------
    
    @Override
    public String fetchRecord(String mdStoreId, String recordId) throws MDStoreServiceException {
        return service.deliverRecord(mdStoreId, recordId);
    }
    
}
