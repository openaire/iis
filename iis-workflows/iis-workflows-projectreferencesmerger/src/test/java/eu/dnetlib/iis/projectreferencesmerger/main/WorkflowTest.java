package eu.dnetlib.iis.projectreferencesmerger.main;

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
    public void testDefaultWorkflow() throws Exception {
        runWorkflow("eu/dnetlib/iis/projectreferencesmerger/main/test/oozie_app");
    }

}
