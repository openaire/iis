package eu.dnetlib.iis.wf.importer.patent;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportPatentWorkflow() {
        testWorkflow("eu/dnetlib/iis/wf/importer/patent/sampletest");
    }
}
