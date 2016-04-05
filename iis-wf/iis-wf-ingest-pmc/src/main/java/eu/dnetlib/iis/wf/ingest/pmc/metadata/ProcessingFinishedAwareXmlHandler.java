package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import org.xml.sax.ContentHandler;

/**
 * Sax xml handler that is aware when it has finished processing xml.
 * 
 * @author madryk
 *
 */
public interface ProcessingFinishedAwareXmlHandler extends ContentHandler {

    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if xml was already parsed.
     */
    boolean hasFinished();
}
