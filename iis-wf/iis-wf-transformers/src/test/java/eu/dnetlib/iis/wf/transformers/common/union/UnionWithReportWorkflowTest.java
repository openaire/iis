package eu.dnetlib.iis.wf.transformers.common.union;

import org.junit.Test;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;

/**
 * @author madryk
 */
public class UnionWithReportWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testUnionWithReportWorkflow() {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/union_with_report/test");
        
    }
}
