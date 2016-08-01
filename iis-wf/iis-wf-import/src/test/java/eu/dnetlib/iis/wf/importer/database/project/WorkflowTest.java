package eu.dnetlib.iis.wf.importer.database.project;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 * Integration test for concepts importer.
 * 
 * @author mhorst
 * 
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportProjectWorkflow() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/database/project/sampletest", wfConf);
    }
}
