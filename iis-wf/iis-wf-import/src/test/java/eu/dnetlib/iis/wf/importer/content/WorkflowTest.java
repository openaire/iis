package eu.dnetlib.iis.wf.importer.content;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Integration test for contents importer.
 * 
 * @author mhorst
 * 
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportContentUrlWorkflow() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/content_url/chain/sampletest", wfConf);
    }
    
    @Test
    public void testImportContentUrlWorkflowWithBlacklisting() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/content_url/core/blacklisting", wfConf);
    }
    
    @Test
    public void testContentUrlDedup() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/content_url/dedup/sampletest", wfConf);
    }
}
