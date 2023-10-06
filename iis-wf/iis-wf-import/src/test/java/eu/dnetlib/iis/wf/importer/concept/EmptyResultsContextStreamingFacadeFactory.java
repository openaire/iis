package eu.dnetlib.iis.wf.importer.concept;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ContextStreamingFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building ISLookup facade mock returning empty results.
 * @author mhorst
 *
 */
public class EmptyResultsContextStreamingFacadeFactory implements ServiceFacadeFactory<ContextStreamingFacade> {

    
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
        public InputStream getStream(String contextId) throws IOException {
            return new ByteArrayInputStream("[]".getBytes());
        }

    }
}
