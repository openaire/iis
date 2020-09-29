package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
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
public class SoftwareUrlRefExtractionWfTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testMainWorkflow() {
        OozieWorkflowTestConfiguration configuration = new OozieWorkflowTestConfiguration();
        configuration.setTimeoutInSeconds(1800);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/softwareurl/main/sampletest", configuration);
    }

    @Test
    public void testMainWorkflowWithoutReferences() {
        OozieWorkflowTestConfiguration configuration = new OozieWorkflowTestConfiguration();
        configuration.setTimeoutInSeconds(1800);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/softwareurl/main/sampletest_without_references");
    }

    @Test
    public void testMainWorkflowEmptyInput() {
        OozieWorkflowTestConfiguration configuration = new OozieWorkflowTestConfiguration();
        configuration.setTimeoutInSeconds(1800);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/softwareurl/main/sampletest_empty_input");
    }
}
