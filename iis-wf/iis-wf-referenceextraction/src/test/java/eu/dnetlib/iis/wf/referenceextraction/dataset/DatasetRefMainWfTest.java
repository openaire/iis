package eu.dnetlib.iis.wf.referenceextraction.dataset;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class DatasetRefMainWfTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/main/sampletest");
	}

    @Test
	public void testMainWorkflowEmptyInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/main/sampletest_empty_input");
	}
    
}
