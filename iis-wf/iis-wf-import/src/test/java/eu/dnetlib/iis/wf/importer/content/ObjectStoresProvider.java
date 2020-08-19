package eu.dnetlib.iis.wf.importer.content;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.data.objectstore.rmi.ObjectStoreService;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides set of all ObjectStore records available in given ObjectStore service.
 * 
 * @author mhorst
 *
 */
public class ObjectStoresProvider {

    private static final Logger logger = LoggerFactory.getLogger(ObjectStoresProvider.class);

    // -------------------- CONSTRUCTORS -------------------------

    private ObjectStoresProvider() {}

    // -------------------- LOGIC --------------------------------
    
    public static void main(String[] args) {
        String objectStoreServiceLocation = "http://beta.services.openaire.eu:8280/is/services/objectStore";
        W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
        eprBuilder.address(objectStoreServiceLocation);
        eprBuilder.build();
        ObjectStoreService objectStore = new JaxwsServiceResolverImpl().getService(ObjectStoreService.class,
                eprBuilder.build());
        logger.info(StringUtils.join(objectStore.getListOfObjectStores(), ','));
    }
}
