package eu.dnetlib.iis.wf.collapsers.basic;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author Dominika Tkaczyk
 * @author Michal Oniszczuk
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDefaultWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/collapsers/basic_collapser/default");
    }

}
