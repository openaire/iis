package eu.dnetlib.iis.workflows.transformers.citationmatching.direct;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
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
	public void testJoin() throws Exception {
    	runWorkflow("eu/dnetlib/iis/transformers/citationmatching/direct/sampledataproducer/oozie_app");
    }

}
