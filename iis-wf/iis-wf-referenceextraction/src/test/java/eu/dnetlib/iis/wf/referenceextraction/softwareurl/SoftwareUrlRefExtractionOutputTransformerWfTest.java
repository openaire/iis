package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

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
public class SoftwareUrlRefExtractionOutputTransformerWfTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testSoftwareUrlTrasformerWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        wf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/softwareurl/output_transformer/test", wf);
    }
}
