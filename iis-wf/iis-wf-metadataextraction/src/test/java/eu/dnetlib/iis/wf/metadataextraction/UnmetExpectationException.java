package eu.dnetlib.iis.wf.metadataextraction;

/**
 * Exception indicating unmet expectation.
 * @author mhorst
 *
 */
public class UnmetExpectationException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;


    public UnmetExpectationException(String message) {
        super(message);
    }


    public UnmetExpectationException(String message, Throwable cause) {
        super(message, cause);
    }


}
