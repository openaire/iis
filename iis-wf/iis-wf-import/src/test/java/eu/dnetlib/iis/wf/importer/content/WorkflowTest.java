package eu.dnetlib.iis.wf.importer.content;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test for contents importer.
 * 
 * @author mhorst
 * 
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportContentUrlWorkflowWithObjectStoreAsBackend() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/content_url/chain/objectstore_based_importer", wfConf);
    }
    
    @Disabled("TODO reenable this test once Hive is installed on CI Test hadoop cluster")
    public void testImportContentUrlWorkflowWithHiveBasedAggregationSubsystemAsBackend() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(1440);
        testWorkflow("eu/dnetlib/iis/wf/importer/content_url/chain/agg_subsystem_based_importer", wfConf);
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
