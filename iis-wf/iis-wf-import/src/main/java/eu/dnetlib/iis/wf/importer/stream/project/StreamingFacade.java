package eu.dnetlib.iis.wf.importer.stream.project;

import java.io.IOException;
import java.io.InputStream;

/**
 * Streaming service facade.
 * @author mhorst
 *
 */
public interface StreamingFacade {

    /**
     * @return underlying stream
     */
    InputStream getStream() throws IOException;
    
}
