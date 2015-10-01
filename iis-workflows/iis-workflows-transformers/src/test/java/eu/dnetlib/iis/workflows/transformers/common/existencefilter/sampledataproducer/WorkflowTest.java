package eu.dnetlib.iis.workflows.transformers.common.existencefilter.sampledataproducer;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author Mateusz Fedoryszak
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        wf.setTimeoutInSeconds(720);
        runWorkflow("eu/dnetlib/iis/workflows/transformers/common/existencefilter/sampledataproducer/oozie_app", wf);
    }

}
