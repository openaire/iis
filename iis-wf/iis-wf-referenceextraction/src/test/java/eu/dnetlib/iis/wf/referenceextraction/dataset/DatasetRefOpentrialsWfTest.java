package eu.dnetlib.iis.wf.referenceextraction.dataset;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
public class DatasetRefOpentrialsWfTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/opentrials/main/sampletest");
	}

    @Test
	public void testMainWorkflowEmptyInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/opentrials/main/sampletest_empty_input");
	}
    
}
