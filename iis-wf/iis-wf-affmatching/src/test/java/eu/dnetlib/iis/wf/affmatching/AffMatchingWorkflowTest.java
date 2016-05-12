package eu.dnetlib.iis.wf.affmatching;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author madryk
 */
@Category(IntegrationTest.class)
public class AffMatchingWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void affMatchingJobTest() {
        
        testWorkflow("eu/dnetlib/iis/wf/affmatching/test");
        
    }
}
