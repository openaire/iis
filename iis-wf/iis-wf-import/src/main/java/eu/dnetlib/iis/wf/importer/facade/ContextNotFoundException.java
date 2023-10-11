package eu.dnetlib.iis.wf.importer.facade;

/**
 * An exception indicating given context was not found.
 * @author mhorst 
 */
public class ContextNotFoundException extends ContextStreamingException {


    private static final long serialVersionUID = -1546075729881700992L;

    public ContextNotFoundException(String contextId) {
        super(contextId);
    }

    
}
