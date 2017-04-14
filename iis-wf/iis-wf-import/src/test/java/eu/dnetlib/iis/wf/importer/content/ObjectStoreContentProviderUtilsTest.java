package eu.dnetlib.iis.wf.importer.content;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class ObjectStoreContentProviderUtilsTest {

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
        byte[] result = ObjectStoreContentProviderUtils.getContentFromURL(url, 1, 1);
        
        // assert
        assertNotNull(result);
        assertEquals(expectedResult, IOUtils.toString(result, encoding));
    }
    
}
