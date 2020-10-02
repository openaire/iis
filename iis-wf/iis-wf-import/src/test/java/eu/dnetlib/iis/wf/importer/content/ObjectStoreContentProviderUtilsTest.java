package eu.dnetlib.iis.wf.importer.content;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class ObjectStoreContentProviderUtilsTest {
    
    private static final String bucketId = "bucket-id";
    
    private static final String keyId = "key-id";
    
    private static final String s3ResourceLoc = ObjectStoreContentProviderUtils.S3_URL_PREFIX + bucketId + '/' + keyId;
    
    @Mock
    private AmazonS3 s3Client;
    
    @Mock
    private S3Object s3Object;
    
    @Mock
    private ObjectMetadata s3ObjectMeta;
    
    @Mock
    private S3ObjectInputStream s3ObjectInputStream;
    
    

    // ---------------------------------- TESTS ----------------------------------
    
    @Test
    public void testExtractResultId() throws Exception {
        // given
        String objectId = "resultId::c9db569cb388e160e4b86ca1ddff84d7";
        
        // execute
        String extractedResultId = ObjectStoreContentProviderUtils.extractResultIdFromObjectId(objectId);
        
        // assert
        assertNotNull(extractedResultId);
        assertEquals("50|" + objectId, extractedResultId);
    }

    @Test
    public void testExtractResultIdNullInput() {
        // given
        String objectId = null;
        
        // execute
        String extractedResultId = ObjectStoreContentProviderUtils.extractResultIdFromObjectId(objectId);
        
        // assert
        assertNull(extractedResultId);
    }
    
    @Test
    public void testExtractResultIdInvalidInput() {
        // given
        String objectId = "resultId::";
        
        // execute
        assertThrows(RuntimeException.class, () -> ObjectStoreContentProviderUtils.extractResultIdFromObjectId(objectId));
    }
    
    @Test
    public void testGenerateObejctId() throws Exception {
        // given
        String resultId = "resultId";
        String url = "http://localhost/";
        String defaultEncoding = "utf8";
        String digestAlgorithm = "md5";
        
        // execute
        String objectId = ObjectStoreContentProviderUtils.generateObjectId(
                resultId, url, defaultEncoding, digestAlgorithm);
        
        // assert
        assertEquals("resultId::c9db569cb388e160e4b86ca1ddff84d7", objectId);
    }
    
    @Test
    public void testGetContentFromURL() throws Exception {
        // given
        String encoding = "utf8";
        String contentClassPath = "/eu/dnetlib/iis/wf/importer/content/sample_data.txt";
        URL url = ObjectStoreContentProviderUtils.class.getResource(contentClassPath);
        String expectedResult = ClassPathResourceProvider.getResourceContent(contentClassPath);
        
        // execute
        byte[] result = ObjectStoreContentProviderUtils.getContentFromURL(url, new ContentRetrievalContext(1, 1, null));
        
        // assert
        assertNotNull(result);
        assertEquals(expectedResult, IOUtils.toString(result, encoding));
    }
    
    @Test
    public void testGetContentFromS3WhenNullClient() {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);

        assertThrows(S3EndpointNotFoundException.class, () ->
                ObjectStoreContentProviderUtils.getContentFromURL(s3ResourceLoc, context));
    }
    
    @Test
    public void testGetContentFromS3WhenInvalidResourceLocation() {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        context.setS3Client(s3Client);

        assertThrows(IOException.class, () ->
                ObjectStoreContentProviderUtils.getContentFromURL("s3://bucket-id", context));
    }
    
    @Test
    public void testGetContentFromS3WhenObjectNotFound() {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        context.setS3Client(s3Client);

        when(s3Client.getObject(argThat(new GetObjectArgumentMatcher()))).thenReturn(null);

        assertThrows(IOException.class, () -> ObjectStoreContentProviderUtils.getContentFromURL(s3ResourceLoc, context));
    }
    
    @Test
    public void testGetContentFromS3WhenSizeIsInvalid() {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        context.setS3Client(s3Client);
        
        when(s3Client.getObject(argThat(new GetObjectArgumentMatcher()))).thenReturn(s3Object);
        when(s3Object.getObjectMetadata()).thenReturn(s3ObjectMeta);
        when(s3ObjectMeta.getContentLength()).thenReturn(Long.valueOf(-1));

        assertThrows(InvalidSizeException.class, () ->
                ObjectStoreContentProviderUtils.getContentFromURL(s3ResourceLoc, context));
    }
    
    @Test
    public void testGetContentFromS3() throws Exception {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        context.setS3Client(s3Client);
        
        when(s3Client.getObject(argThat(new GetObjectArgumentMatcher()))).thenReturn(s3Object);
        when(s3Object.getObjectMetadata()).thenReturn(s3ObjectMeta);
        when(s3ObjectMeta.getContentLength()).thenReturn(Long.valueOf(1));
        when(s3Object.getObjectContent()).thenReturn(s3ObjectInputStream);
        when(s3ObjectInputStream.read(any())).thenReturn(1, IOUtils.EOF);
        
        byte[] result = ObjectStoreContentProviderUtils.getContentFromURL(s3ResourceLoc, context);

        assertNotNull(result);
        assertEquals(result.length, 1);
        assertEquals(result[0], 0);
        
        verify(s3ObjectInputStream, times(1)).close();
    }
    
    // -------------------------------- INNER CLASS -----------------------------------------
    
    
    static class GetObjectArgumentMatcher implements ArgumentMatcher<GetObjectRequest> {

        @Override
        public boolean matches(GetObjectRequest argument) {
            if (argument != null) {
                return keyId.equals(argument.getS3ObjectId().getKey()) &&
                        bucketId.equals(argument.getS3ObjectId().getBucket());
            }
            return false;
        }
        
    }

}
