package eu.dnetlib.iis.wf.citationmatching.direct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.WorkflowTestResult;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class CitationMatchingDirectWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testWorkflow() throws Exception {
        OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        wf.addExpectedOutputAvroDataStore("/report/times");
        wf.setTimeoutInSeconds(720);
        WorkflowTestResult result = testWorkflow("eu/dnetlib/iis/wf/citationmatching/direct/test", wf);
        List<ReportEntry> execTimes = result.getAvroDataStore("/report/times");
        assertEquals(1, execTimes.size());
        assertEquals("export.citationMatching.direct.duration", execTimes.get(0).getKey().toString());
        assertEquals(ReportEntryType.DURATION, execTimes.get(0).getType());
        int durationInMillis = Integer.valueOf(execTimes.get(0).getValue().toString());
        assertTrue(durationInMillis > 1 && durationInMillis < 1000000);
    }

}
