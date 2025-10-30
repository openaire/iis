package eu.dnetlib.iis.wf.metadataextraction;

/**
 * Provisional exception indicating temprorary problems occuring during the metadata extraction process.
 * 
 * @author mhorst
 */
public class ProvisionalException extends Exception {

    private static final long serialVersionUID = -4853084993228697462L;
    
    //------------------------ CONSTRUCTORS -------------------

    public ProvisionalException(String message) {
        super(message);
    }
    
    public ProvisionalException(String message, Throwable cause) {
        super(message, cause);
    }
    

}
