package eu.dnetlib.iis.wf.export.actionmanager.module;

/**
 * Exception raised when no actionset mapping was defined for given algorithm.
 * @author mhorst
 *
 */
public class MappingNotDefinedException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8868977210154868434L;

	
	public MappingNotDefinedException() {
		super();
	}

	public MappingNotDefinedException(String message) {
		super(message);
	}

}
