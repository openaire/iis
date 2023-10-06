package eu.dnetlib.iis.wf.importer.concept;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Integration test for concepts importer.
 * 
 * @author mhorst
 * 
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testImportConceptWithISLookupServiceBasedWorkflow() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/concept/islookup", wfConf);
    }
    
    @Test
    public void testImportConceptWithContextStreamingServiceBasedWorkflow() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/concept/contextservice", wfConf);
    }
}
