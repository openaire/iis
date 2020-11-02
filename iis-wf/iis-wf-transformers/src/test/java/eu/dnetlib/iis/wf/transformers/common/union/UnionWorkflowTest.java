package eu.dnetlib.iis.wf.transformers.common.union;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author madryk
 */
@IntegrationTest
public class UnionWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testUnionWithoutReportWorkflow() {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/union/without_report");
        
    }
    
    @Test
    public void testUnionWithReportWorkflow() {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/union/with_report");
        
    }
}
