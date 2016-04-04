package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.util.Stack;

import org.xml.sax.ContentHandler;

/**
 * Sax xml handler that is aware of parent xml tags 
 * 
 * @author madryk
 *
 */
public interface ParentAwareXmlHandler extends ContentHandler {

    //------------------------ LOGIC --------------------------
    
    /**
     * Returns stack with current parent xml tags
     */
    public Stack<String> getParents();
}
