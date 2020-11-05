package eu.dnetlib.iis.wf.importer.patent;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportPatentWorkflow() {
        testWorkflow("eu/dnetlib/iis/wf/importer/patent/sampletest");
    }
}
