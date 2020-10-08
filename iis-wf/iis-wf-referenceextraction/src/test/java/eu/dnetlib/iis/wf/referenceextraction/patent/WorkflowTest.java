package eu.dnetlib.iis.wf.referenceextraction.patent;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author pjacewicz
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testMainWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/patent/main/sampletest/");
    }

    @Test
    public void testMainWorkflowEmptyTextInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/patent/main/sampletest_empty_text_input/");
    }

    @Test
    public void testMainWorkflowEmptyPatentInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/patent/main/sampletest_empty_patent_input/");
    }
}
