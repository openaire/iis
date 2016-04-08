package eu.dnetlib.iis.wf.documentsclassification;

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
public class DocumentClassificationWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/documentsclassification/sampletest");
	}

    @Test
	public void testMainEmptyInputWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/documentsclassification/sampletest_empty_input");
	}
    
    @Test
	public void testMainEmptyAbstractWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/documentsclassification/sampletest_empty_abstract");
	}
   
}
