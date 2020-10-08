package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
@IntegrationTest
public class SoftwareUrlRefExtractionWfTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testMainWorkflow() {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/softwareurl/main/sampletest");
    }

    @Test
    public void testMainWorkflowWithoutReferences() {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/softwareurl/main/sampletest_without_references");
    }

    @Test
    public void testMainWorkflowEmptyInput() {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/softwareurl/main/sampletest_empty_input");
    }
}
