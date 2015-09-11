package eu.dnetlib.iis.workflows.referenceextraction.pdb;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	runWorkflow("eu/dnetlib/iis/referenceextraction/pdb/main/sampletest/oozie_app");
	}

    @Test
	public void testMainWorkflowWithoutReferences() throws Exception {
        runWorkflow("eu/dnetlib/iis/referenceextraction/pdb/main/sampletest_without_references/oozie_app");
	}

    @Test
	public void testMainWorkflowWithOnlyNullText() throws Exception {
        runWorkflow("eu/dnetlib/iis/referenceextraction/pdb/main/sampletest_with_only_null_text/oozie_app");
	}

    @Test
	public void testMainWorkflowEmptyInput() throws Exception {
        runWorkflow("eu/dnetlib/iis/referenceextraction/pdb/main/sampletest_empty_input/oozie_app");
	}

}
