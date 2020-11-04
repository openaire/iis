package eu.dnetlib.iis.wf.affmatching;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
public class AffMatchingDedupWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void affMatchingDedupTest() {
        
        testWorkflow("eu/dnetlib/iis/wf/affmatching/dedup/test");
        
    }
}
