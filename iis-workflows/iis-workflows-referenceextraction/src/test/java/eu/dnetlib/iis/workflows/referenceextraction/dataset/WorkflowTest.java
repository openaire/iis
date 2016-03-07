package eu.dnetlib.iis.workflows.referenceextraction.dataset;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * 
 * @author Mateusz Kobos
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/dataset/main/sampletest");
	}

    @Test
	public void testMainSQLiteWorkflow() throws Exception{
		testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/dataset/main_sqlite/sampletest");
	}

    @Test
	public void testMainWorkflowWithoutReferences() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/dataset/main/sampletest_without_references");
	}

    @Test
	public void testMainWorkflowWithOnlyNullText() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/dataset/main/sampletest_with_only_null_text");
	}

    @Test
	public void testMainWorkflowEmptyInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/referenceextraction/dataset/main/sampletest_empty_input");
	}
    
}
