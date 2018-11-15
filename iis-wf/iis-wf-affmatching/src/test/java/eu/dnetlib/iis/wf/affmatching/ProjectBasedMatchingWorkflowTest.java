package eu.dnetlib.iis.wf.affmatching;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author mhorst
 */
@Category(IntegrationTest.class)
public class ProjectBasedMatchingWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void projBasedMatchingJobTest() {
        
        testWorkflow("eu/dnetlib/iis/wf/affmatching/projectbased/test");
        
    }
}
