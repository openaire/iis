package eu.dnetlib.iis.wf.importer.concept;

import java.util.Map;

import eu.dnetlib.iis.common.StaticResourceProvider;
import eu.dnetlib.iis.wf.importer.facade.ISLookupFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building ISLookup facade mocks.
 * @author mhorst
 *
 */
public class MockISLookupFacadeFactory implements ServiceFacadeFactory<ISLookupFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ISLookupFacade instantiate(Map<String, String> parameters) {
        return new MockISLookupFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * ISLookup mock providing static concept profiles.
     *
     */
    private static class MockISLookupFacade implements ISLookupFacade {
        
        private static final String profileLocation = "/eu/dnetlib/iis/wf/importer/concept/data/input/fet-fp7.xml";

        //------------------------ LOGIC --------------------------

        @Override
        public Iterable<String> searchProfile(String xPathQuery) throws ServiceFacadeException {
            return StaticResourceProvider.getResourcesContents(profileLocation);
        }

    }
}
