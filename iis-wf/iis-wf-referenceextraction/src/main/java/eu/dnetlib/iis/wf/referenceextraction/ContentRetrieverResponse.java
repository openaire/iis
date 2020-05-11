package eu.dnetlib.iis.wf.referenceextraction;

import java.io.Serializable;

/**
 * Content retriver response.
 * 
 * @author mhorst
 *
 */
public class ContentRetrieverResponse implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -4748043331989529613L;

    /**
     * String content, never set to null.
     */
    private CharSequence content;
    
    private Exception exception;
    
    // ------------------------------ CONSTRUCTORS ----------------------------------
    
    public ContentRetrieverResponse() {
        
    }

    public ContentRetrieverResponse(CharSequence content) {
        this.content = content;
        this.exception = null;
    }
    
    public ContentRetrieverResponse(Exception exception) {
        this.content = "";
        this.exception = exception;
    }

    public CharSequence getContent() {
        return content;
    }

    public void setContent(CharSequence content) {
        this.content = content;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
    
}
