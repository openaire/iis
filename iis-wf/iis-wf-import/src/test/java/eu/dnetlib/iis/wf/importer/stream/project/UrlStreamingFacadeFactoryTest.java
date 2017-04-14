package eu.dnetlib.iis.wf.importer.stream.project;

import static eu.dnetlib.iis.wf.importer.stream.project.UrlStreamingFacadeFactory.IMPORT_PROJECT_STREAM_COMPRESS;
import static eu.dnetlib.iis.wf.importer.stream.project.UrlStreamingFacadeFactory.IMPORT_PROJECT_STREAM_ENDPOINT_URL;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class UrlStreamingFacadeFactoryTest {

    private final UrlStreamingFacadeFactory factory = new UrlStreamingFacadeFactory();
    
    private final Map<String, String> parameters = new HashMap<>();
    
    // --------------------------------------- TESTS ---------------------------------------
    
    @Test(expected=IllegalArgumentException.class)
    public void testInstantiateWithoutEndpointParam() throws Exception {
        // execute
        factory.instantiate(parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testInstantiateWithInvalidEndpointParam() throws Exception {
        // given
        parameters.put(IMPORT_PROJECT_STREAM_ENDPOINT_URL, "invalidEndpoint");
        
        // execute
        factory.instantiate(parameters);
    }
    
    @Test
    public void testInstantiate() throws Exception {
     // given
        parameters.put(IMPORT_PROJECT_STREAM_ENDPOINT_URL, "http://localhost");
        parameters.put(IMPORT_PROJECT_STREAM_COMPRESS, "true");
        
        // execute
        StreamingFacade facade = factory.instantiate(parameters);
        
        // assert
        assertNotNull(facade);
    }

}
