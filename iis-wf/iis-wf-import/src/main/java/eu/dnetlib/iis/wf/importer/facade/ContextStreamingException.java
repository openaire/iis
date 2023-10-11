package eu.dnetlib.iis.wf.importer.facade;

/**
 * An exception indicating fatal error when streaming context content.
 * @author mhorst 
 */
public class ContextStreamingException extends Exception {

    private static final long serialVersionUID = -711409479484772617L;
    
    /**
     * Context identifier.
     */
    private final String contextId;
    
    
    public ContextStreamingException(String contextId) {
        super("Problem occured while streaming context: " + contextId);
        this.contextId = contextId;
    }
    
    public ContextStreamingException(String contextId, Exception e) {
        super("Problem occured while streaming context: " + contextId, e);
        this.contextId = contextId;
    }
    
    public String getContextId() {
        return contextId;
    }

}
