package eu.dnetlib.iis.workflows.transformers.export.documentmetadata;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.WorkflowConfiguration;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testWorkflow() throws Exception {
        WorkflowConfiguration wf = new WorkflowConfiguration();
        wf.setTimeoutInSeconds(720);
        runWorkflow("eu/dnetlib/iis/workflows/transformers/export/documentmetadata/sampledataproducer/oozie_app", wf);
    }

}
