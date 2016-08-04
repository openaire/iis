package eu.dnetlib.iis.common.oozie;

import org.apache.oozie.client.OozieClient;

/**
 * Factory of {@link OozieClient}
 * 
 * @author madryk
 */
public class OozieClientFactory {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns {@link OozieClient} object used for communication with oozie
     */
    public OozieClient createOozieClient(String oozieUrl) {
        
        OozieClient oozieClient = new OozieClient(oozieUrl);
        
        return oozieClient;
    }
}
