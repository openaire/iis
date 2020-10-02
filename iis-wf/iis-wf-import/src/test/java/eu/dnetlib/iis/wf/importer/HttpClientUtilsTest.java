package eu.dnetlib.iis.wf.importer;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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
