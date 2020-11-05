package eu.dnetlib.iis.wf.importer.stream.project;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Integration test for streaming project importer.
 * 
 * @author mhorst
 * 
 */
public class StreamProjectWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportProjectWorkflow() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/stream/project/sampletest", wfConf);
    }
}
