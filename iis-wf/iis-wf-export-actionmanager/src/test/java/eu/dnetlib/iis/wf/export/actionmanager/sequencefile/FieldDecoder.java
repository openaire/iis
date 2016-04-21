package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

/**
 * Field decoder interface.
 * @author mhorst
 *
 * @param <T>
 */
public interface FieldDecoder {

	/**
	 * Decodes given source object into target representation.
	 * @param source
	 * @return decoded object
	 * @throws Exception
	 */
	public Object decode(Object source) throws Exception;
	
	/**
	 * Checks whether decoder can handle given input.
	 * @param source
	 * @return true when decoder can handle given input, false otherwise
	 */
	public boolean canHandle(Object source);
	
}
