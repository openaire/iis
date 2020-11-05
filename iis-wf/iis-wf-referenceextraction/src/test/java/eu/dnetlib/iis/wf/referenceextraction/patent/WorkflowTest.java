package eu.dnetlib.iis.wf.referenceextraction.patent;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author pjacewicz
 */
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
