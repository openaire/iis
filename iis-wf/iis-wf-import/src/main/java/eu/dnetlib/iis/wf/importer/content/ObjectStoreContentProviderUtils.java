package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECT_STORE_S3_ENDPOINT;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;

import com.cloudera.com.amazonaws.services.s3.model.GetObjectRequest;
import com.cloudera.com.amazonaws.services.s3.model.S3Object;
import com.cloudera.com.amazonaws.services.s3.model.S3ObjectId;

import eu.dnetlib.iis.common.InfoSpaceConstants;

/**
 * ObjectStore content provider common utilities.
 * @author mhorst
 *
 */
public final class ObjectStoreContentProviderUtils {

	public static final String defaultEncoding = InfoSpaceConstants.ENCODING_UTF8;
	
	protected static final String objectIdSeparator = "::";
	
	protected static final String S3_URL_PREFIX = "s3://";
	
	
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
			ContentRetrievalContext context) throws IOException, InvalidSizeException, S3EndpointNotFoundException {
	    if (resourceLoc.startsWith(S3_URL_PREFIX)) {
	        return getContentFromS3(resourceLoc, context);
	    } else {
	        return getContentFromURL(new URL(resourceLoc), context);
	    }
	}
	
	private static byte[] getContentFromS3(String resourceLoc,
	        ContentRetrievalContext context) throws IOException, InvalidSizeException, S3EndpointNotFoundException {
	    
	    if (context.getS3Client() != null) {
	        S3ObjectId objId = getObjectId(resourceLoc);
	        S3Object s3Obj = context.getS3Client().getObject(new GetObjectRequest(objId));
	        if (s3Obj != null) {
	            if (s3Obj.getObjectMetadata().getContentLength() > 0) {
	                InputStream is = s3Obj.getObjectContent();
	                try {
	                    return IOUtils.toByteArray(is);
	                } finally {
	                    is.close();
	                }
	            } else {
	                throw new InvalidSizeException();
	            }
	        } else {
	            throw new IOException("S3 object not found for id " + objId + 
	                    " generated based on location: " + resourceLoc);
	        }
	        
	    } else {
	        throw new S3EndpointNotFoundException("no S3 client defined, "
	                + "most probably '" + IMPORT_CONTENT_OBJECT_STORE_S3_ENDPOINT + "' configuration property is missing!");
	    }
	}
	
	private static S3ObjectId getObjectId(String resourceLoc) throws IOException {
	    String[] bucketWithKey = resourceLoc.substring(S3_URL_PREFIX.length(), resourceLoc.length()).split("/", 2);
	    if (bucketWithKey.length == 2) {
	        return new S3ObjectId(bucketWithKey[0], bucketWithKey[1]);    
	    } else {
	        throw new IOException("invalid input resource location: " + resourceLoc);
	    }
	    
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
            ContentRetrievalContext context) throws IOException, InvalidSizeException {
        URLConnection con = url.openConnection();
        con.setConnectTimeout(context.getConnectionTimeout());
        con.setReadTimeout(context.getReadTimeout());
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
