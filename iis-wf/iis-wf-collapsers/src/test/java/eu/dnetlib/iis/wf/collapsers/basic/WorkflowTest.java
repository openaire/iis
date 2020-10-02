package eu.dnetlib.iis.wf.collapsers.basic;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author Dominika Tkaczyk
 * @author Michal Oniszczuk
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDefaultWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/collapsers/basic_collapser/default");
    }

}
