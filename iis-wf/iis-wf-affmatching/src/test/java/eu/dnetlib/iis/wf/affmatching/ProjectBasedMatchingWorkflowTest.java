package eu.dnetlib.iis.wf.affmatching;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
public class ProjectBasedMatchingWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void projBasedMatchingJobTest() {
        
        testWorkflow("eu/dnetlib/iis/wf/affmatching/projectbased/test");
        
    }
}
