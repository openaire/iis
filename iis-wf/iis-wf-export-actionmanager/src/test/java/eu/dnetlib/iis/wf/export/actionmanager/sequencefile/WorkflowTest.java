package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDefaultWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/sequencefile/sampledataproducer");
    }

}
