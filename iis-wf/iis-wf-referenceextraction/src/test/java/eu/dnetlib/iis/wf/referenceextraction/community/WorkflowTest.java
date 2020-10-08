package eu.dnetlib.iis.wf.referenceextraction.community;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/referenceextraction/community/main/sampletest");
	}

    @Test
	public void testMainWorkflowEmptyTextInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/community/main/sampletest_empty_text_input");
	}
    
    @Test
	public void testMainWorkflowEmptyConceptInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/community/main/sampletest_empty_concept_input");
	}

}
