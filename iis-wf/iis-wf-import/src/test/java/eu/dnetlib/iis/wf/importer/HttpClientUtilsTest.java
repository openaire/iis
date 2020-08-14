package eu.dnetlib.iis.wf.importer;

import static org.junit.Assert.assertNotNull;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;

public class HttpClientUtilsTest {


    @Test
    public void testBuildHttpClient() throws Exception {
     // given
        int connectionTimeout = 1;
        int readTimeout = 2;
        
        // execute
        CloseableHttpClient client = HttpClientUtils.buildHttpClient(connectionTimeout, readTimeout);
        
        // assert
        assertNotNull(client);
    }
}
