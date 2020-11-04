package eu.dnetlib.iis.wf.referenceextraction.dataset;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
public class DatasetRefExtractionInputFilterWfTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDatasetFilterWorkflow() throws Exception {
        OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        wf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/dataset/input_filter/test", wf);
    }
}
