package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Integration test for the infospace importer.
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportWorkflow() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/infospace/sampledataproducer", wfConf);
    }
}
