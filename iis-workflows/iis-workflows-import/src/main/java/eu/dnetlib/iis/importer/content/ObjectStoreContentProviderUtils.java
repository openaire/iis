package eu.dnetlib.iis.importer.content;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpDocumentNotFoundException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.iis.common.hbase.HBaseConstants;

/**
 * ObjectStore content provider common utilities.
 * @author mhorst
 *
 */
public class ObjectStoreContentProviderUtils {

	public static final String defaultEncoding = HBaseConstants.STATIC_FIELDS_ENCODING_UTF8;
	
	private static final String objectIdSeparator = "::";
	
	/**
	 * Retrieves object store identifier from remote ISLookup for given repository identifier
	 * @param lookupService
	 * @param repositoryId
	 * @return retrieved object store identifier or null when not found
	 */
	public static String objectStoreIdLookup(ISLookUpService lookupService, String repositoryId) {
		String query = "for $x in //RESOURCE_PROFILE[.//RESOURCE_TYPE/@value = 'LinkedDataDSResourceType' "
				+ "and .//REPOSITORY_SERVICE_IDENTIFIER = '" + repositoryId + "']"
				+ "let $objectStore := $x//DATA_SINK/text()"
				+ "return $objectStore";
		try {
			return lookupService.getResourceProfileByQuery(query).trim();
		} catch (ISLookUpDocumentNotFoundException e) {
			throw new RuntimeException("object store details not found "
					+ "for repository id: " + repositoryId, e);
		} catch (ISLookUpException e) {
			throw new RuntimeException(
					"got exception when looking for object store for "
					+ "repository: " + repositoryId, e);
		}
	}
	
	/**
	 * Returns byte content read from given location.
	 * @param resourceLoc
	 * @param connectionTimeout
	 * @param readTimeout
	 * @return byte content read from given location
	 * @throws IOException
	 */
	public static byte[] getContentFromURL(String resourceLoc,
			int connectionTimeout, int readTimeout) throws IOException {
		if (resourceLoc!=null) {
			URL url = new URL(resourceLoc);
			URLConnection con = url.openConnection();
			con.setConnectTimeout(connectionTimeout);
			con.setReadTimeout(readTimeout);
			return IOUtils.toByteArray(con.getInputStream());
		} else {
			return null;
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
			try {
				StringBuffer strBuff = new StringBuffer();
				strBuff.append(new String(HBaseConstants.ROW_PREFIX_RESULT, 
						HBaseConstants.STATIC_FIELDS_ENCODING_UTF8));
				String[] split = objectId.split(objectIdSeparator);
				if (split.length>=2) {
					strBuff.append(split[0]);
					strBuff.append(objectIdSeparator);
					strBuff.append(split[1]);
					return strBuff.toString();
				} else {
					throw new RuntimeException("invalid object identifier: ");
				}
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		} else {
			return null;
		}
	}

}
