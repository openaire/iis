package eu.dnetlib.iis.wf.transformers.idreplacer;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author Dominika Tkaczyk
 * @author Michal Oniszczuk
 *
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testReplacer1Field() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/transformers/idreplacer/replacer_1_field");
    }

    @Test
	public void testReplacer2Fields() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/transformers/idreplacer/replacer_2_fields");
    }

}
