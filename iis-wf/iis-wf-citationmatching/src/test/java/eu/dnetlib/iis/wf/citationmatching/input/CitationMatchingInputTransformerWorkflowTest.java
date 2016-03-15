package eu.dnetlib.iis.wf.citationmatching.input;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class CitationMatchingInputTransformerWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testJoin() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/citationmatching/input_transformer/test");
    }

}
