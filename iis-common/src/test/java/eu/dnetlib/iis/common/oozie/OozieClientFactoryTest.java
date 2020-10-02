package eu.dnetlib.iis.common.oozie;

import org.apache.oozie.client.OozieClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author madryk
 */
public class OozieClientFactoryTest {

    private OozieClientFactory oozieClientFactory = new OozieClientFactory();
    
    private String oozieUrl = "http://oozieLocation.com:11000/oozie/";
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createOozieClient() {
        
        // execute
        
        OozieClient oozieClient = oozieClientFactory.createOozieClient(oozieUrl);
        
        
        // assert
        
        assertNotNull(oozieClient);
        assertEquals(oozieUrl, oozieClient.getOozieUrl());
        
    }
}
