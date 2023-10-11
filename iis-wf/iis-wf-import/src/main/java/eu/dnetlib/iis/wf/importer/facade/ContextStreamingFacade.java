package eu.dnetlib.iis.wf.importer.facade;

import java.io.InputStream;

/**
 * Context API streaming facade.
 * @author mhorst
 *
 */
public interface ContextStreamingFacade {

    /**
     * Returns stream for a given context identifier. 
     * @return underlying stream
     */
    InputStream getStream(String contextId) throws ContextNotFoundException, ContextStreamingException;
    
}
