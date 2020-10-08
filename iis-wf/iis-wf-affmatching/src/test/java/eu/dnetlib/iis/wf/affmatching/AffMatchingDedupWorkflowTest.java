package eu.dnetlib.iis.wf.affmatching;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
@IntegrationTest
public class AffMatchingDedupWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void affMatchingDedupTest() {
        
        testWorkflow("eu/dnetlib/iis/wf/affmatching/dedup/test");
        
    }
}
