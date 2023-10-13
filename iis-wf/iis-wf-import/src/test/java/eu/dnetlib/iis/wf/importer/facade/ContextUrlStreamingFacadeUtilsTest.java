package eu.dnetlib.iis.wf.importer.facade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
public class ContextUrlStreamingFacadeUtilsTest {

    
    // ------------------------ TESTS --------------------------
    
    @Test
    public void build_url_without_trailing_slash() {
        // given
        String rootUrl = "someRootUrl";
        String contextId = "someCtxId";
        
        // execute
        assertEquals(rootUrl +"/" + contextId, ContextUrlStreamingFacadeUtils.buildUrl(rootUrl, contextId));
    }
    
    @Test
    public void build_url_with_trailing_slash() {
        // given
        String rootUrl = "someRootUrl/";
        String contextId = "someCtxId";
        
        // execute
        assertEquals(rootUrl + contextId, ContextUrlStreamingFacadeUtils.buildUrl(rootUrl, contextId));
    }
    
    @Test
    public void build_url_with_null_context() {
        // given
        String rootUrl = "someRootUrl/";
        String contextId = null;
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> ContextUrlStreamingFacadeUtils.buildUrl(rootUrl, contextId));
    }
    
    @Test
    public void build_url_with_empty_context() {
        // given
        String rootUrl = "someRootUrl/";
        String contextId = "";
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> ContextUrlStreamingFacadeUtils.buildUrl(rootUrl, contextId));
    }
    
    @Test
    public void build_url_with_null_endpoint_location() {
        // given
        String rootUrl = null;
        String contextId = "someCtxId";
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> ContextUrlStreamingFacadeUtils.buildUrl(rootUrl, contextId));
    }
    
    @Test
    public void build_url_with_empty_endpoint_location() {
        // given
        String rootUrl = "";
        String contextId = "someCtxId";
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> ContextUrlStreamingFacadeUtils.buildUrl(rootUrl, contextId));
    }
}
