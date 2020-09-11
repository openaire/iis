package eu.dnetlib.iis.wf.referenceextraction;

/**
 * Exception indicating number of retries exceeded the predefined limit.
 * 
 * @author mhorst
 *
 */
public class RetryLimitExceededException extends Exception {


    private static final long serialVersionUID = -1084913230112190824L;

    
    //------------------------ CONSTRUCTORS -------------------

    public RetryLimitExceededException(String message) {
        super(message);
    }

}
