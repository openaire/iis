package eu.dnetlib.iis.workflows.documentsclassification;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	runWorkflow("eu/dnetlib/iis/documentsclassification/main/sampletest/oozie_app");
	}

    @Test
	public void testMainEmptyInputWorkflow() throws Exception {
    	runWorkflow("eu/dnetlib/iis/documentsclassification/main/sampletest_empty_input/oozie_app");
	}
    
    @Test
	public void testMainEmptyAbstractWorkflow() throws Exception {
    	runWorkflow("eu/dnetlib/iis/documentsclassification/main/sampletest_empty_abstract/oozie_app");
	}
   
    @Test
	public void testMainNullTaxonomyWorkflow() throws Exception {
    	runWorkflow("eu/dnetlib/iis/documentsclassification/main/sampletest_null_taxonomy/oozie_app");
	}
    
}
