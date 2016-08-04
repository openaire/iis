package eu.dnetlib.iis.common.oozie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.oozie.client.OozieClient;
import org.junit.Test;

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
