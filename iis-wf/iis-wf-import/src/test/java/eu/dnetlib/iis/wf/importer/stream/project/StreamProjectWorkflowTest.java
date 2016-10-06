package eu.dnetlib.iis.wf.importer.stream.project;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 * Integration test for streaming project importer.
 * 
 * @author mhorst
 * 
 */
@Category(IntegrationTest.class)
public class StreamProjectWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportProjectWorkflow() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/stream/project/sampletest", wfConf);
    }
}
