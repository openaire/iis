package eu.dnetlib.iis.wf.metadataextraction;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 *
 * @author mhorst
 *
 */
public class MetadataExtractionCacheWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testCreate() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/metadataextraction/cache/create/test",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
	}
    
    @Test
    public void testUpdate() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/metadataextraction/cache/update/test",
                new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
    }
    
    @Test
    public void testChain() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/metadataextraction/cache/chain/test",
                new OozieWorkflowTestConfiguration().setTimeoutInSeconds(2400));
    }
    
    @Test
    public void testChainMetadataExtractionDisabled() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/metadataextraction/cache/chain/extraction_disabled_test",
                new OozieWorkflowTestConfiguration().setTimeoutInSeconds(2400));
    }
    
    @Test
    public void testIdentifyByChecksum() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/metadataextraction/cache/identify_by_checksum/test",
                new OozieWorkflowTestConfiguration().setTimeoutInSeconds(2400));
    }
}
