package eu.dnetlib.iis.wf.importer.concept;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ContextStreamingFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building {@link ContextStreamingFacade} facade mock returning empty results.
 * @author mhorst
 *
 */
public class EmptyResultsContextStreamingFacadeFactory implements ServiceFacadeFactory<ContextStreamingFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ContextStreamingFacade instantiate(Map<String, String> parameters) {
        return new EmptyResultsContextStreamingFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * ISLookup mock providing static concept profiles.
     *
     */
    private static class EmptyResultsContextStreamingFacade implements ContextStreamingFacade {
        
        //------------------------ LOGIC --------------------------
        
        @Override
        public InputStream getStream(String contextId) {
            return new ByteArrayInputStream("[]".getBytes());
        }

    }
}
