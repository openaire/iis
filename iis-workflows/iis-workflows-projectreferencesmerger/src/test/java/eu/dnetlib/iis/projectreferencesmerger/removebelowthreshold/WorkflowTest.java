package eu.dnetlib.iis.projectreferencesmerger.removebelowthreshold;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
    public void testWorkflow() throws Exception {
        runWorkflow("eu/dnetlib/iis/projectreferencesmerger/removebelowthreshold/test/oozie_app");
    }

}
