package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

/**
 * @author mhorst
 *
 */
public class FieldAccessorException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4217545921227110865L;

	public FieldAccessorException(String message){
		super(message);
	}
	
	public FieldAccessorException(String message, Throwable cause){
		super(message, cause);
	}
	
}
