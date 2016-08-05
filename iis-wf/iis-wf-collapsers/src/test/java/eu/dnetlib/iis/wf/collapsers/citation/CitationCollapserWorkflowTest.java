package eu.dnetlib.iis.wf.collapsers.citation;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author mhorst
 */
@Category(IntegrationTest.class)
public class CitationCollapserWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDefaultWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/collapsers/citation/default");
    }


}
