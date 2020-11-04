package eu.dnetlib.iis.wf.referenceextraction.dataset;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author Mateusz Kobos
 *
 */
public class DatasetRefDataciteWfTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/datacite/main/sampletest");
	}

    @Test
	public void testMainSQLiteWorkflow() throws Exception{
		testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/datacite/main_sqlite/sampletest");
	}

    @Test
	public void testMainWorkflowWithoutReferences() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/datacite/main/sampletest_without_references");
	}

    @Test
	public void testMainWorkflowEmptyInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/datacite/main/sampletest_empty_input");
	}
    
}
