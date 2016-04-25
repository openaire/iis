package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

/**
 * Field decoder interface.
 * @author mhorst
 * 
 */
public interface FieldDecoder {

	/**
	 * Decodes given source object into target representation.
	 * @param source object to be decoded
	 * @return decoded object
	 * @throws FieldDecoderException
	 */
	public Object decode(Object source) throws FieldDecoderException;
	
	/**
	 * Checks whether decoder can handle given input.
	 * @param source object to be checked whether it can be handled by decoder
	 * @return true when decoder can handle given input, false otherwise
	 */
	public boolean canHandle(Object source);
	
}
