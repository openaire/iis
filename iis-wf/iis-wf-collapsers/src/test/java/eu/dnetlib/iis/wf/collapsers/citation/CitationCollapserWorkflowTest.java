package eu.dnetlib.iis.wf.collapsers.citation;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
public class CitationCollapserWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDefaultWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/collapsers/citation/default");
    }


}
