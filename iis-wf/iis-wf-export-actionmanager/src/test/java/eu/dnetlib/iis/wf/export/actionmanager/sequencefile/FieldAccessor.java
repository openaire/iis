package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * Accesses object fields using getter methods of field names encoded in properties file as
 * keys. Nested field names are separated with dots.
 * 
 * Notice: field names should start with capital letters as they appear in getter method.
 * 
 * All fields requiring decoder should be marked with '$' prefix. 
 * 
 * @author mhorst
 *
 */
public class FieldAccessor {

	private static final char FIELD_PATH_SEPARATOR = '.';

	private static final char DECODER_PREFIX = '$';

	private static final String GETTER = "get";

	private Map<String, FieldDecoder> decoders = new HashMap<String, FieldDecoder>();

	/**
	 * Registers field decoder.
	 * 
	 * @param field
	 * @param decoder
	 */
	public void registerDecoder(String field, FieldDecoder decoder) {
		decoders.put(field, decoder);
	}

	/**
	 * Extracts value from given object based on fieldPath.
	 * @param fieldPath field names acessible vie getter methods, nested fields are separated with dots
	 * @param object object value should be extracted from
	 * @return extracted value
	 * @throws Exception
	 */
	public Object getValue(String fieldPath, Object object) throws Exception {
		try {
			if (object != null && fieldPath != null) {
				Object currentObject = object;
				String[] tokens = StringUtils.split(fieldPath, FIELD_PATH_SEPARATOR);
				for (String token : tokens) {
					if (token.charAt(0) == DECODER_PREFIX) {
						String fieldName = token.substring(1);
						FieldDecoder decoder = decoders.get(fieldName);
						if (decoder == null) {
							throw new Exception("no decoder registered for " + token);
						}
						Object fieldValue = getFieldValue(fieldName, currentObject);
						if (decoder.canHandle(fieldValue)) {
							currentObject = decoder.decode(fieldValue);
						} else {
							throw new Exception(fieldValue!=null?
									decoder.getClass().getName() + " decoder is not capable of handling object: " + 
									fieldValue.toString() + ", which is " + fieldValue.getClass().getName() + " class instance"
									:decoder.getClass().getName() + " decoder is not capable of handling null object");
						}
					} else if (token.indexOf('[')>0) {
						String fieldName = token.substring(0, token.indexOf('['));
						currentObject = getFieldValue(fieldName, currentObject);
						if (currentObject!=null) {
							 if (currentObject.getClass().isArray()) {
								 currentObject = Array.get(currentObject, getArrayPositionFromToken(token));	 
							 } else if (List.class.isAssignableFrom(currentObject.getClass())) {
								 currentObject = ((List<?>)currentObject).get(getArrayPositionFromToken(token));
							 } else {
								 throw new Exception("unable to extract array or list element "
											+ "based on field path: " + fieldPath + ", "
											+ "object is neither an array nor list:" + 
											currentObject.getClass().getName());
							 }
						} else {
							throw new Exception("unable to extract array or list element "
									+ "based on field path: " + fieldPath + ", "
									+ "object is null!");
						}
					} else {
						currentObject = getFieldValue(token, currentObject);
					}
				}
				return currentObject;
			} else {
				return null;
			}
		} catch (Exception e) {
//			we are extremely verbose to make log analysis process easier 
			throw new Exception("unable to resolve field path '" + fieldPath 
					+ "' on object " + object, e);
		}
	}

	/**
	 * Extracts array/list position from given token
	 * @param token
	 * @return extracted position
	 */
	private static int getArrayPositionFromToken(String token) {
		return Integer.parseInt(StringUtils.substringBetween(token, "[", "]"));
	}
	
	/**
	 * Retrieves value from given object by executing getter method on given field using reflection.
	 * @param fieldName name of the field holding value to be returned
	 * @param object object holding given value inside
	 * @return field value obtained from given object
	 * @throws Exception
	 */
	private static Object getFieldValue(String fieldName, Object object) throws Exception {
		Method method = getMethod(object.getClass(), GETTER + fieldName);
		return method.invoke(object);
	}

	/**
	 * Provides {@link Method} for given class and method name
	 * @param clazz class from which method should be returned
	 * @param name method name
	 * @return declated method
	 * @throws NoSuchFieldException
	 * @throws NoSuchMethodException
	 */
	private static Method getMethod(Class<?> clazz, String name) throws NoSuchFieldException, NoSuchMethodException {
		try {
			return clazz.getDeclaredMethod(name);
		} catch (NoSuchMethodException e) {
			if (clazz == Object.class) {
				throw e;
			}
			return getMethod(clazz.getSuperclass(), name);
		}
	}
}
