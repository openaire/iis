package eu.dnetlib.iis.wf.referenceextraction.pdb;

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
    	testWorkflow("eu/dnetlib/iis/wf/referenceextraction/pdb/main/sampletest");
	}

    @Test
	public void testMainWorkflowWithoutReferences() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/pdb/main/sampletest_without_references");
	}

    @Test
	public void testMainWorkflowEmptyInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/pdb/main/sampletest_empty_input");
	}

}
