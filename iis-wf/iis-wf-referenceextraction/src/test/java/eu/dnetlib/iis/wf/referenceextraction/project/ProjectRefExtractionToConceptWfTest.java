package eu.dnetlib.iis.wf.referenceextraction.project;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
public class ProjectRefExtractionToConceptWfTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testProjectToConceptWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        wf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/project/to_concept/test", wf);
    }
}
