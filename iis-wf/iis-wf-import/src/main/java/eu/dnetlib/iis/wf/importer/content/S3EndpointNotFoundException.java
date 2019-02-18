package eu.dnetlib.iis.wf.importer.content;

/**
 * Exception instance indicating S3 endpoint was not found.
 * @author mhorst
 *
 */
public class S3EndpointNotFoundException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1851820178088169034L;
    
    public S3EndpointNotFoundException(String message) {
        super(message);
    }

}
