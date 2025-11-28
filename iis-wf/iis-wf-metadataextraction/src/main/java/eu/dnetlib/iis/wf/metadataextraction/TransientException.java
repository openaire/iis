package eu.dnetlib.iis.wf.metadataextraction;

/**
 * Transient exception indicating temporary problems occuring during the metadata extraction process.
 * 
 * @author mhorst
 */
public class TransientException extends Exception {

    private static final long serialVersionUID = -4853084993228697462L;
    
    //------------------------ CONSTRUCTORS -------------------

    public TransientException(String message) {
        super(message);
    }
    
    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }
    

}
