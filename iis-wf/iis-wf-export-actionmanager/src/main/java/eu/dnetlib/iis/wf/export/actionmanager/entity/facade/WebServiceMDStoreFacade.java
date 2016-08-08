package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

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

    /**
     * Web service.
     */
    protected final MDStoreService service;
    
    // -------------------- CONSTRUCTORS -------------------------
    
    public WebServiceMDStoreFacade(String serviceLocation) {
        W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
        eprBuilder.address(serviceLocation);
        eprBuilder.build();
        this.service = new JaxwsServiceResolverImpl().getService(MDStoreService.class, eprBuilder.build());
        
    }
    
    // -------------------- PUBLIC -------------------------------
    
    @Override
    public String deliverRecord(String mdStoreId, String recordId) throws MDStoreServiceException {
        return service.deliverRecord(mdStoreId, recordId);
    }
    
}
