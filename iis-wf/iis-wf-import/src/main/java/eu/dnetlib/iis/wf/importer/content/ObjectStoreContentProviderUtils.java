package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;

import eu.dnetlib.iis.common.InfoSpaceConstants;

/**
 * ObjectStore content provider common utilities.
 * @author mhorst
 *
 */
public final class ObjectStoreContentProviderUtils {

	public static final String defaultEncoding = InfoSpaceConstants.ENCODING_UTF8;
	
	protected static final String objectIdSeparator = "::";
	
	
	//-------------------- CONSTRUCTORS -------------------------
	
	private ObjectStoreContentProviderUtils() {}
	
	//-------------------- LOGIC --------------------------------
	
	/**
	 * Returns byte content read from given location.
	 * @param resourceLoc resource location, cannot be null
	 * @param connectionTimeout
	 * @param readTimeout
	 * @return byte content read from given location
	 * @throws InvalidSizeException when size was less or equal 0
	 */
	public static byte[] getContentFromURL(String resourceLoc,
			int connectionTimeout, int readTimeout) throws IOException, InvalidSizeException {
		return getContentFromURL(new URL(resourceLoc), connectionTimeout, readTimeout);
	}
	
	/**
     * Returns byte content read from given location.
     * @param url url pointing to resource
     * @param connectionTimeout
     * @param readTimeout
     * @return byte content read from given location
     * @throws InvalidSizeException when size was less or equal 0
     */
    public static byte[] getContentFromURL(URL url,
            int connectionTimeout, int readTimeout) throws IOException, InvalidSizeException {
        URLConnection con = url.openConnection();
        con.setConnectTimeout(connectionTimeout);
        con.setReadTimeout(readTimeout);
        if (con.getContentLengthLong() > 0) {
            return IOUtils.toByteArray(con.getInputStream());    
        } else {
            throw new InvalidSizeException();
        }
    }
	
	/**
	 * Generates object identifier based on result identifier.
	 * @param resultId
	 * @param url
	 * @param defaultEncoding
	 * @param digestAlgorithm
	 * @return generated object identifier
	 */
	public static String generateObjectId(String resultId, String url,
			String defaultEncoding, String digestAlgorithm) {
		try {
			StringBuilder strBuilder = new StringBuilder(resultId);
			strBuilder.append(objectIdSeparator);
			MessageDigest md = MessageDigest.getInstance(digestAlgorithm);
			md.update(url.getBytes(defaultEncoding));
			strBuilder.append(new String(Hex.encodeHex(md.digest())));
			return strBuilder.toString();	
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Extracts publication identifier from object identifier.
	 * @param objectId
	 * @return publication identifier extracted from object identifier
	 */
	public static String extractResultIdFromObjectId(String objectId) {
		if (objectId!=null && objectId.indexOf(objectIdSeparator)>0) {
			StringBuffer strBuff = new StringBuffer();
			strBuff.append(InfoSpaceConstants.ROW_PREFIX_RESULT);
			String[] split = objectId.split(objectIdSeparator);
			if (split.length>=2) {
				strBuff.append(split[0]);
				strBuff.append(objectIdSeparator);
				strBuff.append(split[1]);
				return strBuff.toString();
			} else {
				throw new RuntimeException("invalid object identifier: ");
			}
		} else {
			return null;
		}
	}

}
