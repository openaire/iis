package eu.dnetlib.iis.wf.referenceextraction.dataset;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class DatasetRefExtractionOutputTransformerWfTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testProjectToConceptWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        wf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/output_transformer/test", wf);
    }
}
