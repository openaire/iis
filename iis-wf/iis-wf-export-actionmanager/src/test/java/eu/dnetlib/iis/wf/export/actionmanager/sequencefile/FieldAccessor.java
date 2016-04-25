package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Accesses object fields using getter methods of field names encoded in properties file as
 * keys. Nested field names are separated with dots.
 * 
 * Notice: for getter method defined as `getFieldName()` expected field name defined in field path is `fieldName`.
 * 
 * All fields requiring decoder should be marked with '$' prefix. 
 * 
 * @author mhorst
 *
 */
public class FieldAccessor {

	private static final char FIELD_PATH_SEPARATOR = '.';

	private static final char DECODER_PREFIX = '$';

	private Map<String, FieldDecoder> decoders = new HashMap<String, FieldDecoder>();

	/**
	 * Registers field decoder.
	 * 
	 * @param field field name for which decoder should be activated
	 * @param decoder decoder to be registered
	 */
	public void registerDecoder(String field, FieldDecoder decoder) {
		decoders.put(field, decoder);
	}

	/**
	 * Extracts value from given object based on fieldPath.
	 * @param fieldPath field names accessible vie getter methods, nested fields are separated with dots
	 * @param object object value should be extracted from
	 * @return extracted value
	 * @throws FieldAccessorException
	 */
	public Object getValue(String fieldPath, Object object) throws FieldAccessorException {
		if (object != null && fieldPath != null) {
			try {
				Object currentObject = object;
				String[] tokens = StringUtils.split(fieldPath, FIELD_PATH_SEPARATOR);
				for (final String token : tokens) {
					String parsedToken = token;
					boolean useDecoder = false;
					Integer arrayOrListPosition = null;
					if (parsedToken.charAt(0) == DECODER_PREFIX) {
						useDecoder = true;
						parsedToken = parsedToken.substring(1);
					}
					if (parsedToken.indexOf('[') > 0) {
						arrayOrListPosition = getArrayPositionFromToken(parsedToken);
						parsedToken = parsedToken.substring(0, parsedToken.indexOf('['));
					}
					currentObject = PropertyUtils.getProperty(currentObject, parsedToken);
					if (useDecoder) {
						currentObject = decode(parsedToken, currentObject);
					}
					if (arrayOrListPosition!=null) {
						currentObject = handleArrayOrList(arrayOrListPosition, currentObject);
					}
				}
				return currentObject;
			} catch (Exception e) {
				// we are extremely verbose to make log analysis process easier
				throw new FieldAccessorException("unable to resolve field path '" + fieldPath + 
						"' on object " + object, e);
			}
		} else {
			throw new FieldAccessorException("Neither fieldPath nor object can be null! " + 
					"Got fieldPath: " + fieldPath + ", object: " + object);
		}
	}

	/**
	 * Decodes source object using decoder defined in token.
	 * @param decoderName decoder name
	 * @param currentObject source object to be decoded
	 * @return decoded object
	 * @throws Exception
	 */
	private Object decode(String decoderName, Object currentObject) throws Exception {
		FieldDecoder decoder = decoders.get(decoderName);
		if (decoder == null) {
			throw new FieldAccessorException("no decoder registered for " + decoderName);
		}
		if (decoder.canHandle(currentObject)) {
			return decoder.decode(currentObject);
		} else {
			throw new FieldAccessorException(currentObject!=null?
					decoder.getClass().getName() + " decoder is not capable of handling object: " + 
					currentObject.toString() + ", which is " + currentObject.getClass().getName() + " class instance"
					:decoder.getClass().getName() + " decoder is not capable of handling null object");
		}
	}
	
	/**
	 * Handles array or list element.
	 * @param position subelement position in array or list
	 * @param currentObject source object
	 * @return field array or list subelement
	 * @throws Exception
	 */
	private static Object handleArrayOrList(int position, Object currentObject) throws Exception {
		if (currentObject!=null) {
			 if (currentObject.getClass().isArray()) {
				 return Array.get(currentObject, position);	 
			 } else if (List.class.isAssignableFrom(currentObject.getClass())) {
				 return ((List<?>)currentObject).get(position);
			 } else {
				 throw new FieldAccessorException("unable to extract array or list element "
							+ "on position: " + position + ", "
							+ "object is neither an array nor list:" + 
							currentObject.getClass().getName());
			 }
		} else {
			throw new FieldAccessorException("unable to extract array or list element "
					+ "on position: " + position + ", object is null!");
		}
	}
	
	/**
	 * Extracts array/list position from given token
	 * @param token field path token including field name and element position markup
	 * @return extracted position
	 */
	private static int getArrayPositionFromToken(String token) {
		return Integer.parseInt(StringUtils.substringBetween(token, "[", "]"));
	}
	
}
