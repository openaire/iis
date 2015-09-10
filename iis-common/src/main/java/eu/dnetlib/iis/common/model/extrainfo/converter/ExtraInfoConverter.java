package eu.dnetlib.iis.common.model.extrainfo.converter;

/**
 * Avro to XML converter module.
 * @author mhorst
 *
 * @param <T>
 */
public interface ExtraInfoConverter<T> {

	/**
	 * Serializes object to its XML representation.
	 * @param object
	 * @return XML representation of avro object
	 */
	String serialize(T object);
	
	/**
	 * Deserializes extra info object.
	 * @param source
	 * @return deserialized object
	 * @throws UnsupportedOperationException
	 */
	T deserialize(String source) throws UnsupportedOperationException;
}
