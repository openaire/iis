package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.Serializable;

import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;

/**
 * {@link Patent} or {@link Exception} wrapper.
 * 
 * @author mhorst
 *
 */
public class PatentOrFault implements Serializable {

    private static final long serialVersionUID = 9084314873157227135L;

    private Patent patent;
    
    private Exception exception;
    
    // ------------------------------ CONSTRUCTORS ----------------------------------
    
    public PatentOrFault() {
        super();
    }

    public PatentOrFault(Patent patent) {
        this.patent = patent;
        this.exception = null;
    }
    
    public PatentOrFault(Patent patent, Exception exception) {
        this.patent = patent;
        this.exception = exception;
    }

    public Patent getPatent() {
        return patent;
    }

    public void setPatent(Patent patent) {
        this.patent = patent;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
    
}

