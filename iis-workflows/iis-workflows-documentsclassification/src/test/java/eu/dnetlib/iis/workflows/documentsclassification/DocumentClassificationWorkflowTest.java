package eu.dnetlib.iis.workflows.documentsclassification;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class DocumentClassificationWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/documentsclassification/main/sampletest");
	}

    @Test
	public void testMainEmptyInputWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/documentsclassification/main/sampletest_empty_input");
	}
    
    @Test
	public void testMainEmptyAbstractWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/documentsclassification/main/sampletest_empty_abstract");
	}
   
    @Test
	public void testMainNullTaxonomyWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/documentsclassification/main/sampletest_null_taxonomy");
	}
    
}
