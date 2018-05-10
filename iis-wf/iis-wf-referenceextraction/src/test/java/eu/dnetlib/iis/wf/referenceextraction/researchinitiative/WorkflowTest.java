package eu.dnetlib.iis.wf.referenceextraction.researchinitiative;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/referenceextraction/researchinitiative/main/sampletest");
	}

    @Test
	public void testMainWorkflowWithoutReferencesInText() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/researchinitiative/main/sampletest_without_references");
	}

    @Test
	public void testMainWorkflowEmptyTextInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/researchinitiative/main/sampletest_empty_text_input");
	}
    
    @Test
	public void testMainWorkflowEmptyMetaInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/researchinitiative/main/sampletest_empty_meta_input");
	}
    
    @Test
	public void testMainWorkflowEmptyConceptInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/researchinitiative/main/sampletest_empty_concept_input");
	}

}
