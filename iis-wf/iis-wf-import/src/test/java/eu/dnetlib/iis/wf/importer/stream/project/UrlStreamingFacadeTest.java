package eu.dnetlib.iis.wf.importer.stream.project;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author mhorst
 *
 */
public class UrlStreamingFacadeTest {

    private final int readTimeout = 60000;

    private final int connectionTimeout = 60000;
    
    private final String contentTxtClassPath = "/eu/dnetlib/iis/wf/importer/content/sample_data.txt";
    
    private final String contentGzClassPath = "/eu/dnetlib/iis/wf/importer/content/sample_data.gz";
    
    
    // --------------------------------------- TESTS ---------------------------------------
    
    @Test
    public void testGetStreamWithoutCompression() throws Exception {
        // given
        boolean compress = false;

        URL url = UrlStreamingFacade.class.getResource(contentTxtClassPath);
        String expectedResult = ClassPathResourceProvider.getResourceContent(contentTxtClassPath);
        
        UrlStreamingFacade facade = new UrlStreamingFacade(url, compress, readTimeout, connectionTimeout);
        
        // execute
        try (InputStream stream = facade.getStream()) {
            // assert
            assertNotNull(stream);
            assertEquals(expectedResult, IOUtils.toString(stream, "utf8"));
        }
    }
    
    @Test
    public void testGetStreamWithCompression() throws Exception {
        // given
        boolean compress = true;

        URL url = UrlStreamingFacade.class.getResource(contentGzClassPath);
        String expectedResult = ClassPathResourceProvider.getResourceContent(contentTxtClassPath);
        
        UrlStreamingFacade facade = new UrlStreamingFacade(url, compress, readTimeout, connectionTimeout);
        
        // execute
        try (InputStream stream = facade.getStream()) {
            // assert
            assertNotNull(stream);
            assertEquals(expectedResult, IOUtils.toString(stream, "utf8"));
        }
    }

}
