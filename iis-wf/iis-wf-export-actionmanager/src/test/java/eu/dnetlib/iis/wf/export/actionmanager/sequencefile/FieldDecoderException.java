package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

/**
 * @author mhorst
 *
 */
public class FieldDecoderException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 550801099880864785L;

	public FieldDecoderException(String message){
		super(message);
	}
	
	public FieldDecoderException(String message, Throwable cause){
		super(message, cause);
	}
	
}
