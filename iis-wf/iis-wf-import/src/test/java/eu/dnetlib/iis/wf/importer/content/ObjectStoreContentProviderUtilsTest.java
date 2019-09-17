package eu.dnetlib.iis.wf.importer.content;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
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
    public void testExtractResultIdNullInput() throws Exception {
        // given
        String objectId = null;
        
        // execute
        String extractedResultId = ObjectStoreContentProviderUtils.extractResultIdFromObjectId(objectId);
        
        // assert
        assertNull(extractedResultId);
    }
    
    @Test(expected=RuntimeException.class)
    public void testExtractResultIdInvalidInput() throws Exception {
        // given
        String objectId = "resultId::";
        
        // execute
        ObjectStoreContentProviderUtils.extractResultIdFromObjectId(objectId);
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
        String expectedResult = IOUtils.toString(
                ObjectStoreContentProviderUtils.class.getResourceAsStream(contentClassPath), encoding);
        
        // execute
        byte[] result = ObjectStoreContentProviderUtils.getContentFromURL(url, new ContentRetrievalContext(1, 1, null));
        
        // assert
        assertNotNull(result);
        assertEquals(expectedResult, IOUtils.toString(result, encoding));
    }
    
    @Test(expected = S3EndpointNotFoundException.class)
    public void testGetContentFromS3WhenNullClient() throws Exception {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        
        ObjectStoreContentProviderUtils.getContentFromURL(s3ResourceLoc, context);
    }
    
    @Test(expected = IOException.class)
    public void testGetContentFromS3WhenInvalidResourceLocation() throws Exception {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        context.setS3Client(s3Client);
        
        ObjectStoreContentProviderUtils.getContentFromURL("s3://bucket-id", context);
    }
    
    @Test(expected = IOException.class)
    public void testGetContentFromS3WhenObjectNotFound() throws Exception {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        context.setS3Client(s3Client);

        when(s3Client.getObject(argThat(new GetObjectArgumentMatcher()))).thenReturn(null);
        
        ObjectStoreContentProviderUtils.getContentFromURL(s3ResourceLoc, context);
    }
    
    @Test(expected = InvalidSizeException.class)
    public void testGetContentFromS3WhenSizeIsInvalid() throws Exception {
        ContentRetrievalContext context = new ContentRetrievalContext(1, 1, 1);
        context.setS3Client(s3Client);
        
        when(s3Client.getObject(argThat(new GetObjectArgumentMatcher()))).thenReturn(s3Object);
        when(s3Object.getObjectMetadata()).thenReturn(s3ObjectMeta);
        when(s3ObjectMeta.getContentLength()).thenReturn(Long.valueOf(-1));
        
        ObjectStoreContentProviderUtils.getContentFromURL(s3ResourceLoc, context);
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
    
    
    static class GetObjectArgumentMatcher extends ArgumentMatcher<GetObjectRequest> {

        @Override
        public boolean matches(Object argument) {
            if (argument instanceof GetObjectRequest) {
                return keyId.equals(((GetObjectRequest) argument).getS3ObjectId().getKey()) &&
                        bucketId.equals(((GetObjectRequest) argument).getS3ObjectId().getBucket());
            }
            return false;
        }
        
    }

}
