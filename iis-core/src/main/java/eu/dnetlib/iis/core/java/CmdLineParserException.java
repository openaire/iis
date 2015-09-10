package eu.dnetlib.iis.core.java;

/**
 * Command line parsing exception
 * @author Mateusz Kobos
 *
 */
public class CmdLineParserException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9219928547611876284L;

	public CmdLineParserException(String message){
		super(message);
	}
	
	public CmdLineParserException(String message, Throwable cause){
		super(message, cause);
	}
}
