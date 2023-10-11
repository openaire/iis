package eu.dnetlib.iis.wf.importer.concept;

import java.io.InputStream;
import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ContextStreamingException;
import eu.dnetlib.iis.wf.importer.facade.ContextStreamingFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building {@link ContextStreamingFacade} facade throwing {@link ContextStreamingException}.
 * @author mhorst
 *
 */
public class ExceptionThrowingContextStreamingFacadeFactory implements ServiceFacadeFactory<ContextStreamingFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ContextStreamingFacade instantiate(Map<String, String> parameters) {
        return new MockISLookupFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * ISLookup mock providing static concept profiles.
     *
     */
    private static class MockISLookupFacade implements ContextStreamingFacade {
        
        //------------------------ LOGIC --------------------------
        
        @Override
        public InputStream getStream(String contextId) throws ContextStreamingException {
            throw new ContextStreamingException(contextId);
        }

    }
}
