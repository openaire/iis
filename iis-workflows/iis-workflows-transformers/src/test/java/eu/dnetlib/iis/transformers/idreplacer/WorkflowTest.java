package eu.dnetlib.iis.transformers.idreplacer;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author Dominika Tkaczyk
 * @author Michal Oniszczuk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
    public void testReplacer1Field() throws Exception {
        runWorkflow("eu/dnetlib/iis/transformers/idreplacer/replacer_1_field/oozie_app");
    }

    @Test
	public void testReplacer2Fields() throws Exception {
    	runWorkflow("eu/dnetlib/iis/transformers/idreplacer/replacer_2_fields/oozie_app");
    }

}
