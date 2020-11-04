package eu.dnetlib.iis.wf.affmatching;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author madryk
 */
public class AffMatchingWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void affMatchingJobTest() {
        
        testWorkflow("eu/dnetlib/iis/wf/affmatching/test");
        
    }
}
