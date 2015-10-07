package eu.dnetlib.iis.core.examples.pig.testing_with_json.removebelowthreshold.minioozie;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/core/examples/pig/testing_with_json/removebelowthreshold/minioozie");
    }

}
