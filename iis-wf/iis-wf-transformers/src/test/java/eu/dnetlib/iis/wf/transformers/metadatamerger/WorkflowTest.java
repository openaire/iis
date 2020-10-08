package eu.dnetlib.iis.wf.transformers.metadatamerger;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testJoin() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/transformers/metadatamerger/sampledataproducer");
    }

}
