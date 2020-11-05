package eu.dnetlib.iis.wf.report;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author madryk
 */
public class ReportWorkflowTest extends AbstractOozieWorkflowTestCase {

    
    //------------------------ TESTS --------------------------
    
    @Test
    public void testReportWorkflow() throws Exception {
        
        testWorkflow("eu/dnetlib/iis/wf/report/builder/test");
        
    }
}
