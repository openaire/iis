package eu.dnetlib.iis.wf.referenceextraction.eoscservice;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/referenceextraction/eoscservice/main/sampletest");
	}

    @Test
	public void testMainWorkflowEmptyTextInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/eoscservice/main/sampletest_empty_text_input");
	}
    
    @Test
	public void testMainWorkflowEmptyServiceInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/eoscservice/main/sampletest_empty_services_input");
	}

}
