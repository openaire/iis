package eu.dnetlib.iis.wf.transformers.common.citations.from.referencemetadata;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author madryk
 */
@Category(IntegrationTest.class)
public class CitationsFromReferenceMetadataWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testWorkflow() throws Exception {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/citations/from/referencemetadata/test");
    }
}
