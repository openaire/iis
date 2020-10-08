package eu.dnetlib.iis.wf.importer.stream.project;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static eu.dnetlib.iis.wf.importer.stream.project.UrlStreamingFacadeFactory.IMPORT_PROJECT_STREAM_COMPRESS;
import static eu.dnetlib.iis.wf.importer.stream.project.UrlStreamingFacadeFactory.IMPORT_PROJECT_STREAM_ENDPOINT_URL;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author mhorst
 *
 */
public class UrlStreamingFacadeFactoryTest {

    private final UrlStreamingFacadeFactory factory = new UrlStreamingFacadeFactory();
    
    private final Map<String, String> parameters = new HashMap<>();
    
    // --------------------------------------- TESTS ---------------------------------------
    
    @Test
    public void testInstantiateWithoutEndpointParam() {
        // execute
        assertThrows(IllegalArgumentException.class, () -> factory.instantiate(parameters));
    }
    
    @Test
    public void testInstantiateWithInvalidEndpointParam() {
        // given
        parameters.put(IMPORT_PROJECT_STREAM_ENDPOINT_URL, "invalidEndpoint");
        
        // execute
        assertThrows(RuntimeException.class, () -> factory.instantiate(parameters));
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
