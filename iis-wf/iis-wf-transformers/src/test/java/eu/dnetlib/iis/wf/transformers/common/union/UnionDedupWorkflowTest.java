package eu.dnetlib.iis.wf.transformers.common.union;

import org.junit.Test;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;

/**
 * @author madryk
 */
public class UnionDedupWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testUnionWithoutReportWorkflow() {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/uniondedup/without_report");
        
    }
    
    @Test
    public void testUnionWithReportWorkflow() {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/uniondedup/with_report");
        
    }
}
