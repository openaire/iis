package eu.dnetlib.iis.wf.primary.report;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author madryk
 */
@Category(IntegrationTest.class)
public class ReportWorkflowTest extends AbstractOozieWorkflowTestCase {

    
    //------------------------ TESTS --------------------------
    
    @Test
    public void testReportWorkflow() throws Exception {
        
        testWorkflow("eu/dnetlib/iis/wf/primary/report/test");
        
    }
}
