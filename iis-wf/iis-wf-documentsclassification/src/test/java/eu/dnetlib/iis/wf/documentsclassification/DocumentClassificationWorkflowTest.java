package eu.dnetlib.iis.wf.documentsclassification;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
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
