package eu.dnetlib.iis.wf.referenceextraction.covid19;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testMainWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/covid19/main/sampletest");
    }

    @Test
    public void testMainWorkflowEmptyInput() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/covid19/main/sampletest_empty_input");
    }

}
