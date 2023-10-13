package eu.dnetlib.iis.wf.importer.concept;

import java.io.InputStream;
import java.util.Map;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.wf.importer.facade.ContextNotFoundException;
import eu.dnetlib.iis.wf.importer.facade.ContextStreamingFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building {@link ContextStreamingFacade} mocks.
 * @author mhorst
 *
 */
public class MockContextStreamingFacadeFactory implements ServiceFacadeFactory<ContextStreamingFacade> {

    private static final String supportedContextId = "fet-fp7";
    
    protected static final String fetProfileLocation = "/eu/dnetlib/iis/wf/importer/concept/data/input/fet-fp7.json";
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ContextStreamingFacade instantiate(Map<String, String> parameters) {
        return new MockContextStreamingFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * ISLookup mock providing static concept profiles.
     *
     */
    private static class MockContextStreamingFacade implements ContextStreamingFacade {
        
        

        //------------------------ LOGIC --------------------------

        @Override
        public InputStream getStream(String contextId) throws ContextNotFoundException {
            if (supportedContextId.equals(contextId)) {
                return ClassPathResourceProvider.getResourceInputStream(fetProfileLocation);
            } else {
                throw new ContextNotFoundException(contextId);
            }
        }

    }
}
