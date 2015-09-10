package eu.dnetlib.iis.core.java;

/**
 * Process exception
 * @author Dominika Tkaczyk
 *
 */
public class ProcessException extends RuntimeException {

	private static final long serialVersionUID = 2758953138374438377L;

	public ProcessException(String message){
		super(message);
	}
	
	public ProcessException(String message, Throwable cause){
		super(message, cause);
	}

}
