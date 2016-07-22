package eu.dnetlib.iis.wf.importer.facade;

/**
 * Service facade generic exception. 
 * 
 * @author mhorst
 *
 */
public class ServiceFacadeException extends Exception {

    private static final long serialVersionUID = 0L;

    //------------------------ CONSTRUCTORS -------------------

    public ServiceFacadeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceFacadeException(String message) {
        super(message);
    }

    public ServiceFacadeException(Throwable cause) {
        super(cause);
    }

}
