package eu.dnetlib.iis.workflows.referenceextraction.researchinitiative;

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
    	testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/researchinitiative/main/sampletest");
	}

    @Test
	public void testMainWorkflowWithoutReferences() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/researchinitiative/main/sampletest_without_references");
	}

    @Test
	public void testMainWorkflowWithOnlyNullText() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/researchinitiative/main/sampletest_with_only_null_text");
	}

    @Test
	public void testMainWorkflowEmptyInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/researchinitiative/main/sampletest_empty_input");
	}

}
