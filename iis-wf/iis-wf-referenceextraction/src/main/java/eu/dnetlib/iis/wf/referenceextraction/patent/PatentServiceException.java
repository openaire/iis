package eu.dnetlib.iis.wf.referenceextraction.patent;

/**
 * Patent service exception.
 * @author mhorst
 *
 */
public class PatentServiceException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -598226111709465990L;

    public PatentServiceException(String message) {
        super(message);
    }

    public PatentServiceException(Throwable cause) {
        super(cause);
    }

}
