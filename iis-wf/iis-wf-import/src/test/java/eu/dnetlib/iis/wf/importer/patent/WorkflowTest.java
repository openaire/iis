package eu.dnetlib.iis.wf.importer.patent;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportPatentWorkflow() {
        testWorkflow("eu/dnetlib/iis/wf/importer/patent/sampletest");
    }
}
